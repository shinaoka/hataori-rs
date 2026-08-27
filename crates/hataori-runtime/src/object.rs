use crate::{
    action::{ActionRegistry, RegisteredAction, Segments},
    domain::{ActionJob, ObjectJob},
    ActionError, Place, RemoteFuture, RuntimeClient, RuntimeError, SpawnOptions, WireValue,
};
use hataori_runtime_foundation::protocol::{
    ActionId, DomainId, LocalityId, ObjectId, ObjectLocation, ObjectTypeId, RunId,
};
use std::{
    any::Any,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    future::Future,
    marker::PhantomData,
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

const OBJECT_MAGIC: [u8; 4] = *b"HOBJ";
const OBJECT_VERSION: u8 = 1;
const OBJECT_HEADER_BYTES: usize = 76;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectConcurrency {
    Exclusive,
    ReadWrite,
}

pub trait DistributedObject: WireValue {
    const TYPE_ID: u128;
}

pub trait ObjectReadAction<T: DistributedObject>: WireValue {
    const ID: u128;
    type Output: WireValue;
    fn execute(self, state: &T) -> Result<Self::Output, ActionError>;
}

pub trait ObjectWriteAction<T: DistributedObject>: WireValue {
    const ID: u128;
    type Output: WireValue;
    fn execute(self, state: &mut T) -> Result<Self::Output, ActionError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mobility {
    Pinned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectAccess {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlacementFallback {
    Any,
    Place(Place),
    Reject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectLimits {
    pub max_objects: usize,
    pub max_mailbox_per_object: usize,
    pub max_resolver_entries: usize,
    pub max_local_leases: usize,
    pub max_roots: usize,
    pub max_placement_tickets: usize,
    pub max_transfers: usize,
    pub lease_renew_interval: Duration,
    pub lease_ttl: Duration,
    pub lease_grace: Duration,
}

impl Default for ObjectLimits {
    fn default() -> Self {
        Self {
            max_objects: 1024,
            max_mailbox_per_object: 64,
            max_resolver_entries: 1024,
            max_local_leases: 1024,
            max_roots: 1024,
            max_placement_tickets: 1024,
            max_transfers: 1024,
            lease_renew_interval: Duration::from_secs(10),
            lease_ttl: Duration::from_secs(30),
            lease_grace: Duration::from_secs(30),
        }
    }
}

impl ObjectLimits {
    pub fn validate(self) -> Result<Self, RuntimeError> {
        if self.max_objects == 0
            || self.max_mailbox_per_object == 0
            || self.max_resolver_entries == 0
            || self.max_local_leases == 0
            || self.max_roots == 0
            || self.max_placement_tickets == 0
            || self.max_transfers == 0
            || self.lease_renew_interval.is_zero()
            || self.lease_ttl <= self.lease_renew_interval
            || self.lease_grace.is_zero()
        {
            return Err(RuntimeError::InvalidLimits("invalid object limits"));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObjectStats {
    pub live_objects: usize,
    pub leases: usize,
    pub suspect_leases: usize,
    pub roots: usize,
    pub queued_calls: usize,
    pub in_flight_calls: usize,
    pub placement_tickets: usize,
    pub resolver_entries: usize,
    pub resolver_bytes: usize,
    pub transfers: usize,
    pub local_calls: u64,
    pub remote_calls: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObjectEnvelope {
    pub(crate) object: ObjectId,
    pub(crate) object_type: ObjectTypeId,
    pub(crate) epoch: u64,
    pub(crate) lease_locality: LocalityId,
    pub(crate) domain: DomainId,
    pub(crate) rooted: bool,
}

impl ObjectEnvelope {
    pub(crate) fn encode(self, mut payload: Segments) -> Segments {
        let mut header = Vec::with_capacity(OBJECT_HEADER_BYTES);
        header.extend_from_slice(&OBJECT_MAGIC);
        header.push(OBJECT_VERSION);
        header.push(u8::from(self.rooted));
        header.extend_from_slice(&[0; 2]);
        header.extend_from_slice(&self.object.run().get().to_le_bytes());
        header.extend_from_slice(&self.object.unique().to_le_bytes());
        header.extend_from_slice(&self.object_type.get().to_le_bytes());
        header.extend_from_slice(&self.epoch.to_le_bytes());
        header.extend_from_slice(&self.lease_locality.get().to_le_bytes());
        header.extend_from_slice(&self.domain.get().to_le_bytes());
        debug_assert_eq!(header.len(), OBJECT_HEADER_BYTES);
        let mut result = Vec::with_capacity(payload.len() + 1);
        result.push(header);
        result.append(&mut payload);
        result
    }

    fn inspect(segments: &Segments) -> Result<Self, ActionError> {
        let header = segments
            .first()
            .ok_or_else(|| ActionError::codec("invalid object envelope length"))?;
        Self::decode_header(header)
    }

    fn decode(mut segments: Segments) -> Result<(Self, Segments), ActionError> {
        if segments.is_empty() || segments[0].len() != OBJECT_HEADER_BYTES {
            return Err(ActionError::codec("invalid object envelope length"));
        }
        let header = segments.remove(0);
        Ok((Self::decode_header(&header)?, segments))
    }

    fn decode_header(header: &[u8]) -> Result<Self, ActionError> {
        if header.len() != OBJECT_HEADER_BYTES {
            return Err(ActionError::codec("invalid object envelope length"));
        }
        if header[..4] != OBJECT_MAGIC || header[4] != OBJECT_VERSION || header[6..8] != [0; 2] {
            return Err(ActionError::codec("invalid object envelope header"));
        }
        let rooted = match header[5] {
            0 => false,
            1 => true,
            _ => return Err(ActionError::codec("invalid object root flag")),
        };
        let run = RunId::new(u128::from_le_bytes(header[8..24].try_into().unwrap()))
            .map_err(|_| ActionError::codec("invalid object run id"))?;
        let object = ObjectId::new(run, u128::from_le_bytes(header[24..40].try_into().unwrap()))
            .map_err(|_| ActionError::codec("invalid object id"))?;
        let object_type =
            ObjectTypeId::new(u128::from_le_bytes(header[40..56].try_into().unwrap()))
                .map_err(|_| ActionError::codec("invalid object type"))?;
        let epoch = u64::from_le_bytes(header[56..64].try_into().unwrap());
        if epoch == 0 {
            return Err(ActionError::codec("invalid object epoch"));
        }
        Ok(Self {
            object,
            object_type,
            epoch,
            lease_locality: LocalityId::new(u64::from_le_bytes(header[64..72].try_into().unwrap())),
            domain: DomainId::new(u32::from_le_bytes(header[72..76].try_into().unwrap())),
            rooted,
        })
    }
}

type Decoder = dyn Fn(Segments) -> Result<ErasedState, ActionError> + Send + Sync;
type LocalWrapper = dyn Fn(Box<dyn Any + Send>) -> Result<ErasedState, RuntimeError> + Send + Sync;
type ObjectHandler = dyn Fn(&ObjectEntry, Segments) -> Result<Segments, ActionError> + Send + Sync;

#[derive(Clone)]
struct ObjectActionRegistration {
    object_type: ObjectTypeId,
    input_schema: u64,
    output_schema: u64,
    access: ObjectAccess,
    handler: Arc<ObjectHandler>,
}

#[derive(Clone)]
struct ObjectTypeRegistration {
    schema: u64,
    concurrency: ObjectConcurrency,
    decoder: Arc<Decoder>,
    local_wrapper: Arc<LocalWrapper>,
}

#[derive(Clone, Default)]
pub(crate) struct ObjectRegistry {
    types: BTreeMap<ObjectTypeId, ObjectTypeRegistration>,
    actions: BTreeMap<ActionId, ObjectActionRegistration>,
}

impl std::fmt::Debug for ObjectRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectRegistry")
            .field("types", &self.types.len())
            .field("actions", &self.actions.len())
            .finish()
    }
}

impl ObjectRegistry {
    pub(crate) fn register_exclusive<T: DistributedObject>(&mut self) -> Result<(), RuntimeError> {
        self.register_type::<T>(
            ObjectConcurrency::Exclusive,
            |segments| {
                Ok(ErasedState::Exclusive(Mutex::new(Box::new(T::decode(
                    segments,
                )?))))
            },
            |state| {
                let state = state.downcast::<T>().map_err(|_| {
                    RuntimeError::UnknownObjectType(ObjectTypeId::new(T::TYPE_ID).unwrap())
                })?;
                Ok(ErasedState::Exclusive(Mutex::new(state)))
            },
        )
    }

    pub(crate) fn register_read_write<T: DistributedObject + Sync>(
        &mut self,
    ) -> Result<(), RuntimeError> {
        self.register_type::<T>(
            ObjectConcurrency::ReadWrite,
            |segments| {
                Ok(ErasedState::ReadWrite(RwLock::new(Box::new(T::decode(
                    segments,
                )?))))
            },
            |state| {
                let state = state.downcast::<T>().map_err(|_| {
                    RuntimeError::UnknownObjectType(ObjectTypeId::new(T::TYPE_ID).unwrap())
                })?;
                let state: Box<dyn Any + Send + Sync> = state;
                Ok(ErasedState::ReadWrite(RwLock::new(state)))
            },
        )
    }

    fn register_type<T: DistributedObject>(
        &mut self,
        concurrency: ObjectConcurrency,
        decoder: impl Fn(Segments) -> Result<ErasedState, ActionError> + Send + Sync + 'static,
        local_wrapper: impl Fn(Box<dyn Any + Send>) -> Result<ErasedState, RuntimeError>
            + Send
            + Sync
            + 'static,
    ) -> Result<(), RuntimeError> {
        let id = ObjectTypeId::new(T::TYPE_ID).map_err(|_| RuntimeError::InvalidObjectTypeId)?;
        if T::SCHEMA_ID == 0 {
            return Err(RuntimeError::InvalidSchema);
        }
        if self.types.contains_key(&id) {
            return Err(RuntimeError::DuplicateObjectType(id));
        }
        let create = create_action_id(id)?;
        if self.reserves_action(create) {
            return Err(RuntimeError::DuplicateAction(create));
        }
        self.types.insert(
            id,
            ObjectTypeRegistration {
                schema: T::SCHEMA_ID,
                concurrency,
                decoder: Arc::new(decoder),
                local_wrapper: Arc::new(local_wrapper),
            },
        );
        Ok(())
    }

    pub(crate) fn register_read<T, A>(&mut self) -> Result<(), RuntimeError>
    where
        T: DistributedObject + Sync,
        A: ObjectReadAction<T>,
    {
        let object_type = self.require_type::<T>(ObjectConcurrency::ReadWrite)?;
        self.insert_action(
            A::ID,
            A::SCHEMA_ID,
            A::Output::SCHEMA_ID,
            object_type,
            ObjectAccess::Read,
            |entry, input| {
                let action = A::decode(input)?;
                let ErasedState::ReadWrite(state) = &entry.state else {
                    return Err(ActionError::user("object does not admit reads"));
                };
                let state = state
                    .try_read()
                    .map_err(|_| ActionError::user("object admission saturated"))?;
                let typed = state
                    .downcast_ref::<T>()
                    .ok_or_else(|| ActionError::user("object type mismatch"))?;
                action.execute(typed)?.encode()
            },
        )
    }

    pub(crate) fn register_write<T, A>(&mut self) -> Result<(), RuntimeError>
    where
        T: DistributedObject,
        A: ObjectWriteAction<T>,
    {
        let object_type =
            ObjectTypeId::new(T::TYPE_ID).map_err(|_| RuntimeError::InvalidObjectTypeId)?;
        if !self.types.contains_key(&object_type) {
            return Err(RuntimeError::UnknownObjectType(object_type));
        }
        self.insert_action(
            A::ID,
            A::SCHEMA_ID,
            A::Output::SCHEMA_ID,
            object_type,
            ObjectAccess::Write,
            |entry, input| {
                let action = A::decode(input)?;
                match &entry.state {
                    ErasedState::Exclusive(state) => {
                        let mut state = state
                            .try_lock()
                            .map_err(|_| ActionError::user("object admission saturated"))?;
                        let typed = state
                            .downcast_mut::<T>()
                            .ok_or_else(|| ActionError::user("object type mismatch"))?;
                        action.execute(typed)?.encode()
                    }
                    ErasedState::ReadWrite(state) => {
                        let mut state = state
                            .try_write()
                            .map_err(|_| ActionError::user("object admission saturated"))?;
                        let typed = state
                            .downcast_mut::<T>()
                            .ok_or_else(|| ActionError::user("object type mismatch"))?;
                        action.execute(typed)?.encode()
                    }
                }
            },
        )
    }

    fn require_type<T: DistributedObject>(
        &self,
        concurrency: ObjectConcurrency,
    ) -> Result<ObjectTypeId, RuntimeError> {
        let id = ObjectTypeId::new(T::TYPE_ID).map_err(|_| RuntimeError::InvalidObjectTypeId)?;
        let registration = self
            .types
            .get(&id)
            .ok_or(RuntimeError::UnknownObjectType(id))?;
        if registration.concurrency != concurrency {
            return Err(RuntimeError::InvalidObjectConcurrency);
        }
        Ok(id)
    }

    fn insert_action(
        &mut self,
        raw_id: u128,
        input_schema: u64,
        output_schema: u64,
        object_type: ObjectTypeId,
        access: ObjectAccess,
        handler: impl Fn(&ObjectEntry, Segments) -> Result<Segments, ActionError>
            + Send
            + Sync
            + 'static,
    ) -> Result<(), RuntimeError> {
        let id = ActionId::new(raw_id).map_err(|_| RuntimeError::InvalidActionId)?;
        if input_schema == 0 || output_schema == 0 || self.reserves_action(id) {
            return Err(RuntimeError::DuplicateAction(id));
        }
        self.actions.insert(
            id,
            ObjectActionRegistration {
                object_type,
                input_schema,
                output_schema,
                access,
                handler: Arc::new(handler),
            },
        );
        Ok(())
    }

    pub(crate) fn reserves_action(&self, id: ActionId) -> bool {
        matches!(
            id.get(),
            LEASE_RENEW_ACTION_ID
                | LEASE_RELEASE_ACTION_ID
                | LEASE_ACQUIRE_ACTION_ID
                | ROOT_RELEASE_ACTION_ID
                | TRANSFER_ACK_ACTION_ID
        ) || self.actions.contains_key(&id)
            || self
                .types
                .keys()
                .any(|object_type| create_action_id(*object_type).is_ok_and(|create| create == id))
    }

    pub(crate) fn install(
        &self,
        service: &Arc<ObjectService>,
        actions: &mut ActionRegistry,
    ) -> Result<(), RuntimeError> {
        actions.insert_erased(
            ActionId::new(LEASE_RENEW_ACTION_ID).unwrap(),
            1,
            1,
            service.lease_handler(true),
        )?;
        actions.insert_erased(
            ActionId::new(LEASE_RELEASE_ACTION_ID).unwrap(),
            1,
            1,
            service.lease_handler(false),
        )?;
        actions.insert_erased(
            ActionId::new(LEASE_ACQUIRE_ACTION_ID).unwrap(),
            1,
            1,
            service.acquire_lease_handler(),
        )?;
        actions.insert_erased(
            ActionId::new(ROOT_RELEASE_ACTION_ID).unwrap(),
            1,
            1,
            service.root_release_handler(),
        )?;
        actions.insert_erased(
            ActionId::new(TRANSFER_ACK_ACTION_ID).unwrap(),
            1,
            1,
            service.transfer_ack_handler(),
        )?;
        for (object_type, registration) in &self.types {
            actions.insert_erased(
                create_action_id(*object_type)?,
                registration.schema,
                LOCATION_SCHEMA_ID,
                service.create_handler(*object_type)?,
            )?;
        }
        for (id, registration) in &self.actions {
            actions.insert_erased(
                *id,
                registration.input_schema,
                registration.output_schema,
                service.action_handler(*id)?,
            )?;
        }
        Ok(())
    }

    pub(crate) fn fingerprint(&self) -> [u8; 32] {
        let mut hashes = [
            0xcbf2_9ce4_8422_2325_u64,
            0x8422_2325_cbf2_9ce4,
            0x9e37_79b9_7f4a_7c15,
            0xd6e8_feb8_6659_fd93,
        ];
        for (id, registration) in &self.types {
            hash_bytes(&mut hashes, &id.get().to_le_bytes());
            hash_bytes(&mut hashes, &registration.schema.to_le_bytes());
            hash_bytes(&mut hashes, &[registration.concurrency as u8]);
        }
        for (id, registration) in &self.actions {
            hash_bytes(&mut hashes, &id.get().to_le_bytes());
            hash_bytes(&mut hashes, &registration.object_type.get().to_le_bytes());
            hash_bytes(&mut hashes, &registration.input_schema.to_le_bytes());
            hash_bytes(&mut hashes, &registration.output_schema.to_le_bytes());
            hash_bytes(&mut hashes, &[registration.access as u8]);
        }
        let mut result = [0; 32];
        for (index, hash) in hashes.into_iter().enumerate() {
            result[index * 8..index * 8 + 8].copy_from_slice(&hash.to_le_bytes());
        }
        result
    }
}

const CREATE_ACTION_MASK: u128 = 0xc4ea_7e00_0000_0000_0000_0000_0000_0000;
const LOCATION_SCHEMA_ID: u64 = 3;
pub(crate) const LEASE_RENEW_ACTION_ID: u128 = 0xc4ea_7e01_0000_0000_0000_0000_0000_0001;
pub(crate) const LEASE_RELEASE_ACTION_ID: u128 = 0xc4ea_7e01_0000_0000_0000_0000_0000_0002;
pub(crate) const LEASE_ACQUIRE_ACTION_ID: u128 = 0xc4ea_7e01_0000_0000_0000_0000_0000_0003;
pub(crate) const ROOT_RELEASE_ACTION_ID: u128 = 0xc4ea_7e01_0000_0000_0000_0000_0000_0004;
pub(crate) const TRANSFER_ACK_ACTION_ID: u128 = 0xc4ea_7e01_0000_0000_0000_0000_0000_0005;

pub(crate) fn create_action_id(object_type: ObjectTypeId) -> Result<ActionId, RuntimeError> {
    ActionId::new(object_type.get() ^ CREATE_ACTION_MASK).map_err(|_| RuntimeError::InvalidActionId)
}

fn hash_bytes(hashes: &mut [u64; 4], bytes: &[u8]) {
    for byte in bytes {
        for (index, hash) in hashes.iter_mut().enumerate() {
            *hash ^= u64::from(*byte).wrapping_add(index as u64);
            *hash = hash.wrapping_mul(0x100_0000_01b3);
        }
    }
}

enum ErasedState {
    Exclusive(Mutex<Box<dyn Any + Send>>),
    ReadWrite(RwLock<Box<dyn Any + Send + Sync>>),
}

struct LocalAdmissionPin<'a> {
    service: &'a ObjectService,
    job: ObjectJob,
}

impl Drop for LocalAdmissionPin<'_> {
    fn drop(&mut self) {
        self.service.complete(self.job);
        self.service.collect(self.job.object, Instant::now());
    }
}

struct LeaseRecord {
    expires_at: Instant,
    suspect_since: Option<Instant>,
}

struct Admission {
    readers: usize,
    writer: bool,
    queue: VecDeque<ActionJob>,
}

struct ObjectEntry {
    object_type: ObjectTypeId,
    location: ObjectLocation,
    state: ErasedState,
    leases: Mutex<HashMap<LocalityId, LeaseRecord>>,
    roots: Mutex<usize>,
    in_flight: Mutex<usize>,
    placement_tickets: Mutex<usize>,
    admission: Mutex<Admission>,
}

pub(crate) struct ObjectService {
    run_id: RunId,
    local_id: LocalityId,
    pub(crate) limits: ObjectLimits,
    registry: ObjectRegistry,
    entries: Mutex<HashMap<ObjectId, Arc<ObjectEntry>>>,
    next_slot: Mutex<u64>,
    resolver: Mutex<(HashMap<ObjectId, ObjectLocation>, VecDeque<ObjectId>)>,
    local_calls: AtomicU64,
    remote_calls: AtomicU64,
    root_count: AtomicU64,
    transfer_count: AtomicU64,
    placement_count: AtomicU64,
    pending_transfers: Mutex<HashSet<(ObjectId, LocalityId)>>,
}

impl std::fmt::Debug for ObjectService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats(Instant::now());
        f.debug_struct("ObjectService")
            .field("local_id", &self.local_id)
            .field("stats", &stats)
            .finish()
    }
}

impl ObjectService {
    pub(crate) fn new(
        run_id: RunId,
        local_id: LocalityId,
        limits: ObjectLimits,
        registry: ObjectRegistry,
    ) -> Self {
        Self {
            run_id,
            local_id,
            limits,
            registry,
            entries: Mutex::new(HashMap::new()),
            next_slot: Mutex::new(1),
            resolver: Mutex::new((HashMap::new(), VecDeque::new())),
            local_calls: AtomicU64::new(0),
            remote_calls: AtomicU64::new(0),
            root_count: AtomicU64::new(0),
            transfer_count: AtomicU64::new(0),
            placement_count: AtomicU64::new(0),
            pending_transfers: Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn placement_ticket(
        self: &Arc<Self>,
        lease: Arc<LocalLease>,
    ) -> Result<PlacementTicket, RuntimeError> {
        self.placement_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.limits.max_placement_tickets as u64).then_some(count + 1)
            })
            .map_err(|_| RuntimeError::ResourceExhausted {
                resource: crate::ResourceKind::PlacementTickets,
                limit: self.limits.max_placement_tickets,
            })?;
        Ok(PlacementTicket {
            service: Arc::clone(self),
            _lease: lease,
        })
    }

    pub(crate) fn create_local<T: DistributedObject>(
        &self,
        object: ObjectId,
        object_type: ObjectTypeId,
        domain: DomainId,
        state: T,
        lease_locality: LocalityId,
        rooted: bool,
    ) -> Result<ObjectLocation, RuntimeError> {
        let registration = self
            .registry
            .types
            .get(&object_type)
            .ok_or(RuntimeError::UnknownObjectType(object_type))?;
        let state = (registration.local_wrapper)(Box::new(state))?;
        self.insert(
            ObjectEnvelope {
                object,
                object_type,
                epoch: 1,
                lease_locality,
                domain,
                rooted,
            },
            state,
            Instant::now(),
        )
        .map_err(|error| RuntimeError::ObjectAction(error.message()))
    }

    pub(crate) fn create_handler(
        self: &Arc<Self>,
        object_type: ObjectTypeId,
    ) -> Result<RegisteredAction, RuntimeError> {
        let registration = self
            .registry
            .types
            .get(&object_type)
            .cloned()
            .ok_or(RuntimeError::UnknownObjectType(object_type))?;
        let service = Arc::clone(self);
        Ok(RegisteredAction::new(move |segments| {
            let (envelope, state) = ObjectEnvelope::decode(segments)?;
            if envelope.object_type != object_type || envelope.object.run() != service.run_id {
                return Err(ActionError::user("object identity mismatch"));
            }
            let state = (registration.decoder)(state)?;
            let location = service.insert(envelope, state, Instant::now())?;
            encode_location(location)
        }))
    }

    pub(crate) fn acquire_lease_handler(self: &Arc<Self>) -> RegisteredAction {
        let service = Arc::clone(self);
        RegisteredAction::new(move |segments| {
            let (envelope, payload) = ObjectEnvelope::decode(segments)?;
            let acquired = if envelope.rooted {
                service.begin_transfer(envelope.object, envelope.lease_locality, Instant::now())
            } else {
                service.acquire(envelope.object, envelope.lease_locality, Instant::now())
            };
            if !payload.is_empty() || !acquired {
                return Err(ActionError::user("object lease is unavailable"));
            }
            Ok(Vec::new())
        })
    }

    pub(crate) fn transfer_ack_handler(self: &Arc<Self>) -> RegisteredAction {
        let service = Arc::clone(self);
        RegisteredAction::new(move |segments| {
            let (envelope, payload) = ObjectEnvelope::decode(segments)?;
            if !payload.is_empty() {
                return Err(ActionError::codec(
                    "transfer acknowledgement payload must be empty",
                ));
            }
            service.ack_transfer(envelope.object, envelope.lease_locality);
            Ok(Vec::new())
        })
    }

    pub(crate) fn root_release_handler(self: &Arc<Self>) -> RegisteredAction {
        let service = Arc::clone(self);
        RegisteredAction::new(move |segments| {
            let (envelope, payload) = ObjectEnvelope::decode(segments)?;
            if !payload.is_empty() {
                return Err(ActionError::codec("root control payload must be empty"));
            }
            service.release_root(envelope.object);
            Ok(Vec::new())
        })
    }

    pub(crate) fn lease_handler(self: &Arc<Self>, renew: bool) -> RegisteredAction {
        let service = Arc::clone(self);
        RegisteredAction::new(move |segments| {
            let (envelope, payload) = ObjectEnvelope::decode(segments)?;
            if !payload.is_empty() {
                return Err(ActionError::codec("lease control payload must be empty"));
            }
            if renew {
                if !service.renew(envelope.object, envelope.lease_locality, Instant::now()) {
                    return Err(ActionError::user("object lease is unavailable"));
                }
            } else {
                service.release(envelope.object, envelope.lease_locality);
            }
            Ok(Vec::new())
        })
    }

    pub(crate) fn admit(
        &self,
        action: ActionId,
        mut job: ActionJob,
        domain_available: bool,
    ) -> Result<Option<ActionJob>, RuntimeError> {
        let Some(registration) = self.registry.actions.get(&action) else {
            return Ok(Some(job));
        };
        let envelope = ObjectEnvelope::inspect(&job.input)
            .map_err(|error| RuntimeError::Protocol(error.message()))?;
        let entry = self
            .entries
            .lock()
            .unwrap()
            .get(&envelope.object)
            .cloned()
            .ok_or(RuntimeError::ObjectCollected(envelope.object))?;
        if entry.object_type != envelope.object_type {
            return Err(RuntimeError::UnknownObjectType(envelope.object_type));
        }
        if entry.location.epoch() != envelope.epoch {
            return Err(RuntimeError::StaleObjectLocation(envelope.object));
        }
        let read = registration.access == ObjectAccess::Read;
        let mut admission = entry.admission.lock().unwrap();
        let available = domain_available
            && admission.queue.is_empty()
            && if read {
                !admission.writer
            } else {
                !admission.writer && admission.readers == 0
            };
        job.object = Some(ObjectJob {
            object: envelope.object,
            read,
        });
        if available {
            if read {
                admission.readers += 1;
            } else {
                admission.writer = true;
            }
            *entry.in_flight.lock().unwrap() += 1;
            return Ok(Some(job));
        }
        if admission.queue.len() >= self.limits.max_mailbox_per_object {
            return Err(RuntimeError::ResourceExhausted {
                resource: crate::ResourceKind::ObjectMailbox,
                limit: self.limits.max_mailbox_per_object,
            });
        }
        admission.queue.push_back(job);
        Ok(None)
    }

    pub(crate) fn complete(&self, completed: ObjectJob) -> Vec<ActionJob> {
        let Some(entry) = self.entries.lock().unwrap().get(&completed.object).cloned() else {
            return Vec::new();
        };
        let mut admission = entry.admission.lock().unwrap();
        if completed.read {
            admission.readers = admission.readers.saturating_sub(1);
        } else {
            admission.writer = false;
        }
        let mut in_flight = entry.in_flight.lock().unwrap();
        *in_flight = in_flight.saturating_sub(1);
        Vec::new()
    }

    pub(crate) fn rollback(&self, job: ObjectJob) -> Vec<ActionJob> {
        self.complete(job)
    }

    pub(crate) fn retry_ready(
        &self,
        max: usize,
        mut domain_available: impl FnMut(DomainId) -> bool,
    ) -> Vec<ActionJob> {
        let entries: Vec<_> = self.entries.lock().unwrap().values().cloned().collect();
        let mut ready = Vec::new();
        for entry in entries {
            if ready.len() >= max {
                break;
            }
            let mut admission = entry.admission.lock().unwrap();
            if admission.writer || admission.readers != 0 {
                continue;
            }
            loop {
                if ready.len() >= max {
                    break;
                }
                let Some(front) = admission.queue.front() else {
                    break;
                };
                if !domain_available(front.domain) {
                    break;
                }
                let object = front
                    .object
                    .expect("queued object job has admission metadata");
                if object.read {
                    admission.readers += 1;
                } else {
                    admission.writer = true;
                }
                *entry.in_flight.lock().unwrap() += 1;
                ready.push(admission.queue.pop_front().unwrap());
                if !object.read {
                    break;
                }
            }
        }
        ready
    }

    fn admit_local<'a>(
        &'a self,
        entry: &ObjectEntry,
        object: ObjectId,
        read: bool,
    ) -> Result<LocalAdmissionPin<'a>, RuntimeError> {
        let mut admission = entry.admission.lock().unwrap();
        let available = admission.queue.is_empty()
            && if read {
                !admission.writer
            } else {
                !admission.writer && admission.readers == 0
            };
        if !available {
            return Err(RuntimeError::ResourceExhausted {
                resource: crate::ResourceKind::ObjectMailbox,
                limit: self.limits.max_mailbox_per_object,
            });
        }
        if read {
            admission.readers += 1;
        } else {
            admission.writer = true;
        }
        *entry.in_flight.lock().unwrap() += 1;
        Ok(LocalAdmissionPin {
            service: self,
            job: ObjectJob { object, read },
        })
    }

    pub(crate) fn call_read_local<T, A>(
        &self,
        object: ObjectId,
        object_type: ObjectTypeId,
        location: ObjectLocation,
        action: A,
    ) -> Result<A::Output, RuntimeError>
    where
        T: DistributedObject + Sync,
        A: ObjectReadAction<T>,
    {
        let entry = self.local_entry(object, object_type, location)?;
        let _pin = self.admit_local(&entry, object, true)?;
        let ErasedState::ReadWrite(state) = &entry.state else {
            return Err(RuntimeError::InvalidObjectConcurrency);
        };
        let state = state
            .try_read()
            .map_err(|_| RuntimeError::ResourceExhausted {
                resource: crate::ResourceKind::ObjectMailbox,
                limit: self.limits.max_mailbox_per_object,
            })?;
        let typed = state
            .downcast_ref::<T>()
            .ok_or(RuntimeError::UnknownObjectType(object_type))?;
        self.local_calls.fetch_add(1, Ordering::Relaxed);
        match catch_unwind(AssertUnwindSafe(|| action.execute(typed))) {
            Ok(result) => result.map_err(|error| RuntimeError::ObjectAction(error.message())),
            Err(_) => Err(RuntimeError::ObjectAction("object action panicked".into())),
        }
    }

    pub(crate) fn call_write_local<T, A>(
        &self,
        object: ObjectId,
        object_type: ObjectTypeId,
        location: ObjectLocation,
        action: A,
    ) -> Result<A::Output, RuntimeError>
    where
        T: DistributedObject,
        A: ObjectWriteAction<T>,
    {
        let entry = self.local_entry(object, object_type, location)?;
        let _pin = self.admit_local(&entry, object, false)?;
        let output = match &entry.state {
            ErasedState::Exclusive(state) => {
                let mut state = state
                    .try_lock()
                    .map_err(|_| RuntimeError::ResourceExhausted {
                        resource: crate::ResourceKind::ObjectMailbox,
                        limit: self.limits.max_mailbox_per_object,
                    })?;
                let typed = state
                    .downcast_mut::<T>()
                    .ok_or(RuntimeError::UnknownObjectType(object_type))?;
                catch_unwind(AssertUnwindSafe(|| action.execute(typed)))
            }
            ErasedState::ReadWrite(state) => {
                let mut state = state
                    .try_write()
                    .map_err(|_| RuntimeError::ResourceExhausted {
                        resource: crate::ResourceKind::ObjectMailbox,
                        limit: self.limits.max_mailbox_per_object,
                    })?;
                let typed = state
                    .downcast_mut::<T>()
                    .ok_or(RuntimeError::UnknownObjectType(object_type))?;
                catch_unwind(AssertUnwindSafe(|| action.execute(typed)))
            }
        };
        self.local_calls.fetch_add(1, Ordering::Relaxed);
        match output {
            Ok(result) => result.map_err(|error| RuntimeError::ObjectAction(error.message())),
            Err(_) => Err(RuntimeError::ObjectAction("object action panicked".into())),
        }
    }

    fn local_entry(
        &self,
        object: ObjectId,
        object_type: ObjectTypeId,
        location: ObjectLocation,
    ) -> Result<Arc<ObjectEntry>, RuntimeError> {
        let entry = self
            .entries
            .lock()
            .unwrap()
            .get(&object)
            .cloned()
            .ok_or(RuntimeError::ObjectCollected(object))?;
        if entry.object_type != object_type {
            return Err(RuntimeError::UnknownObjectType(object_type));
        }
        if entry.location.epoch() != location.epoch() {
            return Err(RuntimeError::StaleObjectLocation(object));
        }
        Ok(entry)
    }

    pub(crate) fn action_handler(
        self: &Arc<Self>,
        action: ActionId,
    ) -> Result<RegisteredAction, RuntimeError> {
        let registration = self
            .registry
            .actions
            .get(&action)
            .cloned()
            .ok_or(RuntimeError::UnknownAction(action))?;
        let service = Arc::clone(self);
        Ok(RegisteredAction::new(move |segments| {
            let (envelope, input) = ObjectEnvelope::decode(segments)?;
            if envelope.object_type != registration.object_type {
                return Err(ActionError::user("object type mismatch"));
            }
            service.remote_calls.fetch_add(1, Ordering::Relaxed);
            let entry = service
                .entries
                .lock()
                .unwrap()
                .get(&envelope.object)
                .cloned()
                .ok_or_else(|| ActionError::user("object collected"))?;
            if entry.object_type != envelope.object_type || entry.location.epoch() != envelope.epoch
            {
                return Err(ActionError::user("stale object location"));
            }
            (registration.handler)(&entry, input)
        }))
    }

    fn insert(
        &self,
        envelope: ObjectEnvelope,
        state: ErasedState,
        now: Instant,
    ) -> Result<ObjectLocation, ActionError> {
        let object = envelope.object;
        let object_type = envelope.object_type;
        let mut entries = self.entries.lock().unwrap();
        if entries.len() >= self.limits.max_objects {
            return Err(ActionError::user("object store is full"));
        }
        if entries.contains_key(&object) {
            return Err(ActionError::user("duplicate object id"));
        }
        let mut slot = self.next_slot.lock().unwrap();
        let current = *slot;
        *slot = slot
            .checked_add(1)
            .ok_or_else(|| ActionError::user("object slot overflow"))?;
        let location = ObjectLocation::new(self.local_id, envelope.domain, current, 1, 1)
            .map_err(|_| ActionError::user("invalid object location"))?;
        if envelope.rooted
            && self
                .root_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    (count < self.limits.max_roots as u64).then_some(count + 1)
                })
                .is_err()
        {
            return Err(ActionError::user("object root limit reached"));
        }
        let mut leases = HashMap::new();
        leases.insert(
            envelope.lease_locality,
            LeaseRecord {
                expires_at: now + self.limits.lease_ttl,
                suspect_since: None,
            },
        );
        entries.insert(
            object,
            Arc::new(ObjectEntry {
                object_type,
                location,
                state,
                leases: Mutex::new(leases),
                roots: Mutex::new(usize::from(envelope.rooted)),
                in_flight: Mutex::new(0),
                placement_tickets: Mutex::new(0),
                admission: Mutex::new(Admission {
                    readers: 0,
                    writer: false,
                    queue: VecDeque::new(),
                }),
            }),
        );
        drop(entries);
        self.cache_location(object, location);
        Ok(location)
    }

    pub(crate) fn cache_location(&self, object: ObjectId, location: ObjectLocation) {
        let mut resolver = self.resolver.lock().unwrap();
        if !resolver.0.contains_key(&object) {
            if resolver.0.len() >= self.limits.max_resolver_entries {
                if let Some(oldest) = resolver.1.pop_front() {
                    resolver.0.remove(&oldest);
                }
            }
            resolver.1.push_back(object);
        }
        resolver.0.insert(object, location);
    }

    pub(crate) fn resolve(&self, object: ObjectId) -> Option<ObjectLocation> {
        self.resolver.lock().unwrap().0.get(&object).copied()
    }

    pub(crate) fn clear_all(&self) {
        self.entries.lock().unwrap().clear();
        self.root_count.store(0, Ordering::Release);
        self.transfer_count.store(0, Ordering::Release);
        self.placement_count.store(0, Ordering::Release);
        self.pending_transfers.lock().unwrap().clear();
        self.clear_resolver();
    }

    pub(crate) fn clear_resolver(&self) {
        let mut resolver = self.resolver.lock().unwrap();
        resolver.0.clear();
        resolver.1.clear();
    }

    pub(crate) fn begin_transfer(
        &self,
        object: ObjectId,
        locality: LocalityId,
        now: Instant,
    ) -> bool {
        let key = (object, locality);
        if self
            .entries
            .lock()
            .unwrap()
            .get(&object)
            .is_none_or(|entry| entry.leases.lock().unwrap().contains_key(&locality))
        {
            return false;
        }
        {
            let mut transfers = self.pending_transfers.lock().unwrap();
            if transfers.contains(&key) {
                return false;
            }
            if self
                .transfer_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    (count < self.limits.max_transfers as u64).then_some(count + 1)
                })
                .is_err()
            {
                return false;
            }
            transfers.insert(key);
        }
        if self.acquire(object, locality, now) {
            true
        } else {
            self.pending_transfers.lock().unwrap().remove(&key);
            self.transfer_count.fetch_sub(1, Ordering::AcqRel);
            false
        }
    }

    pub(crate) fn ack_transfer(&self, object: ObjectId, locality: LocalityId) -> bool {
        if self
            .pending_transfers
            .lock()
            .unwrap()
            .remove(&(object, locality))
        {
            self.transfer_count.fetch_sub(1, Ordering::AcqRel);
            true
        } else {
            false
        }
    }

    pub(crate) fn acquire(&self, object: ObjectId, locality: LocalityId, now: Instant) -> bool {
        let Some(entry) = self.entries.lock().unwrap().get(&object).cloned() else {
            return false;
        };
        let mut leases = entry.leases.lock().unwrap();
        if leases.len() >= self.limits.max_local_leases && !leases.contains_key(&locality) {
            return false;
        }
        leases.insert(
            locality,
            LeaseRecord {
                expires_at: now + self.limits.lease_ttl,
                suspect_since: None,
            },
        );
        true
    }

    pub(crate) fn release_root(&self, object: ObjectId) -> bool {
        let Some(entry) = self.entries.lock().unwrap().get(&object).cloned() else {
            return false;
        };
        let mut roots = entry.roots.lock().unwrap();
        if *roots == 0 {
            return false;
        }
        *roots -= 1;
        self.root_count.fetch_sub(1, Ordering::AcqRel);
        drop(roots);
        self.collect(object, Instant::now());
        true
    }

    pub(crate) fn renew(&self, object: ObjectId, locality: LocalityId, now: Instant) -> bool {
        let Some(entry) = self.entries.lock().unwrap().get(&object).cloned() else {
            return false;
        };
        let mut leases = entry.leases.lock().unwrap();
        let Some(lease) = leases.get_mut(&locality) else {
            return false;
        };
        lease.expires_at = now + self.limits.lease_ttl;
        lease.suspect_since = None;
        true
    }

    pub(crate) fn release(&self, object: ObjectId, locality: LocalityId) -> bool {
        let Some(entry) = self.entries.lock().unwrap().get(&object).cloned() else {
            return false;
        };
        let removed = entry.leases.lock().unwrap().remove(&locality).is_some();
        self.ack_transfer(object, locality);
        self.collect(object, Instant::now());
        removed
    }

    pub(crate) fn expire(&self, now: Instant) {
        let ids: Vec<_> = self.entries.lock().unwrap().keys().copied().collect();
        for id in ids {
            let Some(entry) = self.entries.lock().unwrap().get(&id).cloned() else {
                continue;
            };
            let mut leases = entry.leases.lock().unwrap();
            for lease in leases.values_mut() {
                if lease.expires_at <= now && lease.suspect_since.is_none() {
                    lease.suspect_since = Some(now);
                }
            }
            leases.retain(|_, lease| {
                lease.suspect_since.is_none_or(|since| {
                    now.saturating_duration_since(since) < self.limits.lease_grace
                })
            });
            let active: HashSet<_> = leases.keys().copied().collect();
            drop(leases);
            let mut transfers = self.pending_transfers.lock().unwrap();
            let before = transfers.len();
            transfers.retain(|(object, locality)| *object != id || active.contains(locality));
            let removed = before - transfers.len();
            if removed != 0 {
                self.transfer_count
                    .fetch_sub(removed as u64, Ordering::AcqRel);
            }
            drop(transfers);
            self.collect(id, now);
        }
    }

    fn collect(&self, object: ObjectId, _now: Instant) {
        let mut entries = self.entries.lock().unwrap();
        let collectible = entries.get(&object).is_some_and(|entry| {
            entry.leases.lock().unwrap().is_empty()
                && *entry.roots.lock().unwrap() == 0
                && *entry.in_flight.lock().unwrap() == 0
                && *entry.placement_tickets.lock().unwrap() == 0
        });
        if collectible {
            entries.remove(&object);
            self.resolver.lock().unwrap().0.remove(&object);
        }
    }

    pub(crate) fn stats(&self, now: Instant) -> ObjectStats {
        let entries = self.entries.lock().unwrap();
        let resolver_entries = self.resolver.lock().unwrap().0.len();
        let mut stats = ObjectStats {
            live_objects: entries.len(),
            resolver_entries,
            resolver_bytes: resolver_entries
                .saturating_mul(std::mem::size_of::<(ObjectId, ObjectLocation)>()),
            local_calls: self.local_calls.load(Ordering::Relaxed),
            remote_calls: self.remote_calls.load(Ordering::Relaxed),
            transfers: self.transfer_count.load(Ordering::Relaxed) as usize,
            placement_tickets: self.placement_count.load(Ordering::Relaxed) as usize,
            ..ObjectStats::default()
        };
        for entry in entries.values() {
            let leases = entry.leases.lock().unwrap();
            stats.leases += leases.len();
            stats.suspect_leases += leases
                .values()
                .filter(|lease| lease.expires_at <= now || lease.suspect_since.is_some())
                .count();
            stats.roots += *entry.roots.lock().unwrap();
            stats.in_flight_calls += *entry.in_flight.lock().unwrap();
            stats.queued_calls += entry.admission.lock().unwrap().queue.len();
            stats.placement_tickets += *entry.placement_tickets.lock().unwrap();
        }
        stats
    }
}

fn decode_location(bytes: Vec<u8>) -> Result<ObjectLocation, RuntimeError> {
    if bytes.len() != 32 {
        return Err(RuntimeError::Protocol(
            "invalid object location payload".into(),
        ));
    }
    ObjectLocation::new(
        LocalityId::new(u64::from_le_bytes(bytes[0..8].try_into().unwrap())),
        DomainId::new(u32::from_le_bytes(bytes[8..12].try_into().unwrap())),
        u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
        u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
        u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
    )
    .map_err(|error| RuntimeError::Protocol(format!("invalid object location: {error}")))
}

pub(crate) struct LocalLease {
    object: ObjectId,
    object_type: ObjectTypeId,
    location: ObjectLocation,
    client: RuntimeClient,
    active: AtomicBool,
    next_renewal: Mutex<Instant>,
}

impl LocalLease {
    fn new(
        object: ObjectId,
        object_type: ObjectTypeId,
        location: ObjectLocation,
        client: RuntimeClient,
        renew_interval: Duration,
    ) -> Self {
        Self {
            object,
            object_type,
            location,
            client,
            active: AtomicBool::new(true),
            next_renewal: Mutex::new(Instant::now() + renew_interval),
        }
    }

    pub(crate) fn renew(&self) {
        self.client
            .renew_object(self.object, self.object_type, self.location);
    }

    pub(crate) fn deactivate(&self) {
        if self.active.swap(false, Ordering::AcqRel) {
            self.client
                .release_object(self.object, self.object_type, self.location);
        }
    }
}

impl Drop for LocalLease {
    fn drop(&mut self) {
        if self.active.swap(false, Ordering::AcqRel) {
            self.client
                .release_object(self.object, self.object_type, self.location);
        }
    }
}

pub(crate) struct LocalLeaseManager {
    limit: usize,
    renew_interval: Duration,
    entries: Mutex<HashMap<ObjectId, Weak<LocalLease>>>,
}

impl LocalLeaseManager {
    pub(crate) fn new(limit: usize, renew_interval: Duration) -> Self {
        Self {
            limit,
            renew_interval,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn attach<T: DistributedObject>(
        &self,
        object: ObjectId,
        object_type: ObjectTypeId,
        location: ObjectLocation,
        client: RuntimeClient,
    ) -> Result<Remote<T>, RuntimeError> {
        let mut entries = self.entries.lock().unwrap();
        if let Some(existing) = entries.get(&object).and_then(Weak::upgrade) {
            return Ok(Remote {
                object,
                object_type,
                location,
                client,
                lease: existing,
                marker: PhantomData,
            });
        }
        entries.retain(|_, lease| lease.strong_count() != 0);
        if entries.len() >= self.limit {
            return Err(RuntimeError::ResourceExhausted {
                resource: crate::ResourceKind::LocalityLeases,
                limit: self.limit,
            });
        }
        let lease = Arc::new(LocalLease::new(
            object,
            object_type,
            location,
            client.clone(),
            self.renew_interval,
        ));
        entries.insert(object, Arc::downgrade(&lease));
        Ok(Remote {
            object,
            object_type,
            location,
            client,
            lease,
            marker: PhantomData,
        })
    }

    pub(crate) fn due(&self, now: Instant) -> Vec<Arc<LocalLease>> {
        let mut entries = self.entries.lock().unwrap();
        let mut due = Vec::new();
        entries.retain(|_, weak| {
            let Some(lease) = weak.upgrade() else {
                return false;
            };
            let mut next = lease.next_renewal.lock().unwrap();
            if lease.active.load(Ordering::Acquire) && *next <= now {
                *next = now + self.renew_interval;
                due.push(Arc::clone(&lease));
            }
            true
        });
        due
    }

    pub(crate) fn clear(&self) {
        let leases: Vec<_> = self
            .entries
            .lock()
            .unwrap()
            .drain()
            .filter_map(|(_, lease)| lease.upgrade())
            .collect();
        for lease in leases {
            lease.deactivate();
        }
    }
}

pub(crate) struct PlacementTicket {
    service: Arc<ObjectService>,
    _lease: Arc<LocalLease>,
}

impl Drop for PlacementTicket {
    fn drop(&mut self) {
        self.service.placement_count.fetch_sub(1, Ordering::AcqRel);
    }
}

#[must_use = "a colocated future must be driven or dropped"]
pub struct ColocatedFuture<T: WireValue> {
    inner: RemoteFuture<T>,
    ticket: Option<PlacementTicket>,
}

impl<T: WireValue> ColocatedFuture<T> {
    pub(crate) fn new(inner: RemoteFuture<T>, ticket: PlacementTicket) -> Self {
        Self {
            inner,
            ticket: Some(ticket),
        }
    }
}

impl<T: WireValue> Future for ColocatedFuture<T> {
    type Output = Result<T, RuntimeError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let result = Pin::new(&mut self.inner).poll(context);
        if result.is_ready() {
            self.ticket.take();
        }
        result
    }
}

impl<T: WireValue> Unpin for ColocatedFuture<T> {}

enum PlacementState<T: WireValue> {
    Preferred(ColocatedFuture<T>),
    Fallback(RemoteFuture<T>),
    Done,
}

#[must_use = "a placement future must be driven or dropped"]
pub struct PlacementFuture<T: WireValue> {
    state: PlacementState<T>,
    fallback: PlacementFallback,
    client: RuntimeClient,
    action: ActionId,
    payload: Option<Segments>,
    options: SpawnOptions,
}

impl<T: WireValue> PlacementFuture<T> {
    pub(crate) fn fallback(
        future: RemoteFuture<T>,
        fallback: PlacementFallback,
        client: RuntimeClient,
        action: ActionId,
        options: SpawnOptions,
    ) -> Self {
        Self {
            state: PlacementState::Fallback(future),
            fallback,
            client,
            action,
            payload: None,
            options,
        }
    }

    pub(crate) fn new(
        preferred: ColocatedFuture<T>,
        fallback: PlacementFallback,
        client: RuntimeClient,
        action: ActionId,
        payload: Segments,
        options: SpawnOptions,
    ) -> Self {
        Self {
            state: PlacementState::Preferred(preferred),
            fallback,
            client,
            action,
            payload: Some(payload),
            options,
        }
    }
}

impl<T: WireValue> Future for PlacementFuture<T> {
    type Output = Result<T, RuntimeError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match &mut self.state {
                PlacementState::Preferred(future) => match Pin::new(future).poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(value)) => {
                        self.state = PlacementState::Done;
                        return Poll::Ready(Ok(value));
                    }
                    Poll::Ready(Err(error)) if placement_saturated(&error) => {
                        let place = match self.fallback {
                            PlacementFallback::Any => {
                                Place::new(self.client.local_id(), DomainId::DEFAULT)
                            }
                            PlacementFallback::Place(place) => place,
                            PlacementFallback::Reject => {
                                self.state = PlacementState::Done;
                                return Poll::Ready(Err(error));
                            }
                        };
                        let payload = self.payload.take().expect("placement fallback used once");
                        match self
                            .client
                            .spawn_encoded(place, self.action, payload, self.options)
                        {
                            Ok(future) => self.state = PlacementState::Fallback(future),
                            Err(error) => {
                                self.state = PlacementState::Done;
                                return Poll::Ready(Err(error));
                            }
                        }
                    }
                    Poll::Ready(Err(error)) => {
                        self.state = PlacementState::Done;
                        return Poll::Ready(Err(error));
                    }
                },
                PlacementState::Fallback(future) => {
                    let result = Pin::new(future).poll(context);
                    if result.is_ready() {
                        self.state = PlacementState::Done;
                    }
                    return result;
                }
                PlacementState::Done => {
                    return Poll::Ready(Err(RuntimeError::Protocol(
                        "placement future polled after completion".into(),
                    )));
                }
            }
        }
    }
}

fn placement_saturated(error: &RuntimeError) -> bool {
    matches!(
        error,
        RuntimeError::ResourceExhausted { .. } | RuntimeError::Placement { .. }
    ) || matches!(error, RuntimeError::RemoteAction { message, .. }
            if message.contains("ResourceExhausted")
                || message.contains("saturated")
                || message.contains("queue"))
}

impl<T: WireValue> Unpin for PlacementFuture<T> {}

#[must_use = "an object call future must be driven or dropped"]
pub enum ObjectCallFuture<T: WireValue> {
    Local(Option<Result<T, RuntimeError>>),
    Remote(RemoteFuture<T>),
}

impl<T: WireValue> std::fmt::Debug for ObjectCallFuture<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectCallFuture").finish_non_exhaustive()
    }
}

impl<T: WireValue> Future for ObjectCallFuture<T> {
    type Output = Result<T, RuntimeError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut *self {
            Self::Local(result) => Poll::Ready(result.take().unwrap_or_else(|| {
                Err(RuntimeError::Protocol(
                    "object future polled after completion".into(),
                ))
            })),
            Self::Remote(future) => Pin::new(future).poll(context),
        }
    }
}

impl<T: WireValue> Unpin for ObjectCallFuture<T> {}

#[must_use = "an object creation future must be driven or dropped"]
pub struct CreateFuture<T: DistributedObject> {
    inner: Option<RemoteFuture<Vec<u8>>>,
    ready: Option<Result<Remote<T>, RuntimeError>>,
    object: ObjectId,
    object_type: ObjectTypeId,
    client: RuntimeClient,
}

impl<T: DistributedObject> std::fmt::Debug for CreateFuture<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateFuture")
            .field("object", &self.object)
            .finish_non_exhaustive()
    }
}

impl<T: DistributedObject> CreateFuture<T> {
    pub(crate) fn new(
        inner: RemoteFuture<Vec<u8>>,
        object: ObjectId,
        object_type: ObjectTypeId,
        client: RuntimeClient,
    ) -> Self {
        Self {
            inner: Some(inner),
            ready: None,
            object,
            object_type,
            client,
        }
    }

    pub(crate) fn ready(
        result: Result<Remote<T>, RuntimeError>,
        object: ObjectId,
        object_type: ObjectTypeId,
        client: RuntimeClient,
    ) -> Self {
        Self {
            inner: None,
            ready: Some(result),
            object,
            object_type,
            client,
        }
    }
}

impl<T: DistributedObject> Future for CreateFuture<T> {
    type Output = Result<Remote<T>, RuntimeError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.ready.take() {
            return Poll::Ready(result);
        }
        let poll = Pin::new(
            self.inner
                .as_mut()
                .expect("create future polled after completion"),
        )
        .poll(context);
        match poll {
            Poll::Ready(Ok(bytes)) => {
                self.inner = None;
                Poll::Ready(decode_location(bytes).and_then(|location| {
                    self.client
                        .attach_object(self.object, self.object_type, location)
                }))
            }
            Poll::Ready(Err(error)) => {
                self.inner = None;
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: DistributedObject> Unpin for CreateFuture<T> {}

#[must_use = "a weak upgrade future must be driven or dropped"]
pub struct UpgradeFuture<T: DistributedObject> {
    inner: RemoteFuture<()>,
    object: ObjectId,
    object_type: ObjectTypeId,
    location: ObjectLocation,
    client: RuntimeClient,
    marker: PhantomData<T>,
}

impl<T: DistributedObject> UpgradeFuture<T> {
    pub(crate) fn new(
        inner: RemoteFuture<()>,
        object: ObjectId,
        object_type: ObjectTypeId,
        location: ObjectLocation,
        client: RuntimeClient,
    ) -> Self {
        Self {
            inner,
            object,
            object_type,
            location,
            client,
            marker: PhantomData,
        }
    }
}

impl<T: DistributedObject> Future for UpgradeFuture<T> {
    type Output = Result<Remote<T>, RuntimeError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(context) {
            Poll::Ready(Ok(())) => Poll::Ready(self.client.attach_object(
                self.object,
                self.object_type,
                self.location,
            )),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: DistributedObject> Unpin for UpgradeFuture<T> {}

pub struct LeaseTransfer<T: DistributedObject> {
    pub(crate) object: ObjectId,
    pub(crate) object_type: ObjectTypeId,
    pub(crate) location: ObjectLocation,
    pub(crate) destination: LocalityId,
    pub(crate) expires_at: Instant,
    source_client: RuntimeClient,
    pub(crate) active: bool,
    marker: PhantomData<T>,
}

impl<T: DistributedObject> std::fmt::Debug for LeaseTransfer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseTransfer")
            .field("object", &self.object)
            .field("destination", &self.destination)
            .finish_non_exhaustive()
    }
}

impl<T: DistributedObject> Drop for LeaseTransfer<T> {
    fn drop(&mut self) {
        if self.active {
            self.source_client.release_for_locality(
                self.object,
                self.object_type,
                self.location,
                self.destination,
            );
        }
    }
}

#[must_use = "a lease transfer future must be driven or dropped"]
pub struct TransferFuture<T: DistributedObject> {
    inner: RemoteFuture<()>,
    object: ObjectId,
    object_type: ObjectTypeId,
    location: ObjectLocation,
    destination: LocalityId,
    expires_at: Instant,
    source_client: RuntimeClient,
    marker: PhantomData<T>,
}

impl<T: DistributedObject> TransferFuture<T> {
    pub(crate) fn new(
        inner: RemoteFuture<()>,
        object: ObjectId,
        object_type: ObjectTypeId,
        location: ObjectLocation,
        destination: LocalityId,
        expires_at: Instant,
        source_client: RuntimeClient,
    ) -> Self {
        Self {
            inner,
            object,
            object_type,
            location,
            destination,
            expires_at,
            source_client,
            marker: PhantomData,
        }
    }
}

impl<T: DistributedObject> Future for TransferFuture<T> {
    type Output = Result<LeaseTransfer<T>, RuntimeError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(context) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(LeaseTransfer {
                object: self.object,
                object_type: self.object_type,
                location: self.location,
                destination: self.destination,
                expires_at: self.expires_at,
                source_client: self.source_client.clone(),
                active: true,
                marker: PhantomData,
            })),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: DistributedObject> Unpin for TransferFuture<T> {}

fn encode_location(location: ObjectLocation) -> Result<Segments, ActionError> {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&location.locality().get().to_le_bytes());
    bytes.extend_from_slice(&location.domain().get().to_le_bytes());
    bytes.extend_from_slice(&location.slot().to_le_bytes());
    bytes.extend_from_slice(&location.generation().to_le_bytes());
    bytes.extend_from_slice(&location.epoch().to_le_bytes());
    Ok(vec![bytes])
}

pub struct Remote<T: DistributedObject> {
    pub(crate) object: ObjectId,
    pub(crate) object_type: ObjectTypeId,
    pub(crate) location: ObjectLocation,
    pub(crate) client: RuntimeClient,
    pub(crate) lease: Arc<LocalLease>,
    marker: PhantomData<T>,
}

impl<T: DistributedObject> Clone for Remote<T> {
    fn clone(&self) -> Self {
        Self {
            object: self.object,
            object_type: self.object_type,
            location: self.location,
            client: self.client.clone(),
            lease: Arc::clone(&self.lease),
            marker: PhantomData,
        }
    }
}

impl<T: DistributedObject> std::fmt::Debug for Remote<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Remote")
            .field("object", &self.object)
            .field("object_type", &self.object_type)
            .finish_non_exhaustive()
    }
}

impl<T: DistributedObject> Remote<T> {
    pub const fn object_id(&self) -> ObjectId {
        self.object
    }

    pub const fn observed_location(&self) -> ObjectLocation {
        self.location
    }

    /// Releases this strong handle. The locality lease is released when this
    /// is the last local clone.
    pub fn release(self) {
        drop(self);
    }

    pub fn call_read<A: ObjectReadAction<T>>(
        &self,
        action: A,
    ) -> Result<ObjectCallFuture<A::Output>, RuntimeError>
    where
        T: Sync,
    {
        self.client.call_object_read(self, action)
    }

    pub fn call_write<A: ObjectWriteAction<T>>(
        &self,
        action: A,
    ) -> Result<ObjectCallFuture<A::Output>, RuntimeError> {
        self.client.call_object_write(self, action)
    }

    pub fn transfer_to(&self, destination: LocalityId) -> Result<TransferFuture<T>, RuntimeError> {
        self.client.transfer_object(self, destination)
    }

    pub fn downgrade(&self) -> WeakRemote<T> {
        WeakRemote {
            object: self.object,
            object_type: self.object_type,
            location: self.location,
            marker: PhantomData,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct WeakRemote<T: DistributedObject> {
    pub(crate) object: ObjectId,
    pub(crate) object_type: ObjectTypeId,
    pub(crate) location: ObjectLocation,
    marker: PhantomData<T>,
}

impl<T: DistributedObject> Copy for WeakRemote<T> {}

impl<T: DistributedObject> Clone for WeakRemote<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: DistributedObject> WireValue for WeakRemote<T> {
    const SCHEMA_ID: u64 = T::SCHEMA_ID ^ 0x5745_414b_4f42_4a00;

    fn encode(self) -> Result<Segments, ActionError> {
        let mut bytes = Vec::with_capacity(80);
        bytes.extend_from_slice(&self.object.run().get().to_le_bytes());
        bytes.extend_from_slice(&self.object.unique().to_le_bytes());
        bytes.extend_from_slice(&self.object_type.get().to_le_bytes());
        bytes.extend_from_slice(&self.location.locality().get().to_le_bytes());
        bytes.extend_from_slice(&self.location.domain().get().to_le_bytes());
        bytes.extend_from_slice(&self.location.slot().to_le_bytes());
        bytes.extend_from_slice(&self.location.generation().to_le_bytes());
        bytes.extend_from_slice(&self.location.epoch().to_le_bytes());
        Ok(vec![bytes])
    }

    fn decode(mut segments: Segments) -> Result<Self, ActionError> {
        if segments.len() != 1 || segments[0].len() != 80 {
            return Err(ActionError::codec("invalid weak remote payload"));
        }
        let bytes = segments.pop().unwrap();
        let run = RunId::new(u128::from_le_bytes(bytes[0..16].try_into().unwrap()))
            .map_err(|_| ActionError::codec("invalid weak remote run"))?;
        let object = ObjectId::new(run, u128::from_le_bytes(bytes[16..32].try_into().unwrap()))
            .map_err(|_| ActionError::codec("invalid weak remote object"))?;
        let object_type = ObjectTypeId::new(u128::from_le_bytes(bytes[32..48].try_into().unwrap()))
            .map_err(|_| ActionError::codec("invalid weak remote type"))?;
        if object_type.get() != T::TYPE_ID {
            return Err(ActionError::codec("weak remote type mismatch"));
        }
        let location = ObjectLocation::new(
            LocalityId::new(u64::from_le_bytes(bytes[48..56].try_into().unwrap())),
            DomainId::new(u32::from_le_bytes(bytes[56..60].try_into().unwrap())),
            u64::from_le_bytes(bytes[60..68].try_into().unwrap()),
            u32::from_le_bytes(bytes[68..72].try_into().unwrap()),
            u64::from_le_bytes(bytes[72..80].try_into().unwrap()),
        )
        .map_err(|_| ActionError::codec("invalid weak remote location"))?;
        Ok(Self {
            object,
            object_type,
            location,
            marker: PhantomData,
        })
    }
}

impl<T: DistributedObject> WeakRemote<T> {
    pub const fn object_id(self) -> ObjectId {
        self.object
    }

    pub fn upgrade(&self, client: &RuntimeClient) -> Result<UpgradeFuture<T>, RuntimeError> {
        client.upgrade_object(self)
    }
}

pub struct ObjectRoot<T: DistributedObject> {
    object: ObjectId,
    object_type: ObjectTypeId,
    location: ObjectLocation,
    client: RuntimeClient,
    active: bool,
    marker: PhantomData<T>,
}

impl<T: DistributedObject> std::fmt::Debug for ObjectRoot<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectRoot")
            .field("object", &self.object)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl<T: DistributedObject> ObjectRoot<T> {
    pub(crate) fn new(remote: &Remote<T>) -> Self {
        Self {
            object: remote.object,
            object_type: remote.object_type,
            location: remote.location,
            client: remote.client.clone(),
            active: true,
            marker: PhantomData,
        }
    }

    pub const fn object_id(&self) -> ObjectId {
        self.object
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.active {
            self.active = false;
            self.client
                .release_root(self.object, self.object_type, self.location);
        }
    }
}

impl<T: DistributedObject> Drop for ObjectRoot<T> {
    fn drop(&mut self) {
        self.release_inner();
    }
}

#[must_use = "a rooted object creation future must be driven or dropped"]
pub struct RootedCreateFuture<T: DistributedObject> {
    inner: CreateFuture<T>,
}

impl<T: DistributedObject> RootedCreateFuture<T> {
    pub(crate) fn new(inner: CreateFuture<T>) -> Self {
        Self { inner }
    }
}

impl<T: DistributedObject> Future for RootedCreateFuture<T> {
    type Output = Result<(Remote<T>, ObjectRoot<T>), RuntimeError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(context) {
            Poll::Ready(Ok(remote)) => {
                let root = ObjectRoot::new(&remote);
                Poll::Ready(Ok((remote, root)))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: DistributedObject> Unpin for RootedCreateFuture<T> {}

#[cfg(test)]
mod tests;
