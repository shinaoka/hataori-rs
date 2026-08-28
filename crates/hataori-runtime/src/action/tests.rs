use super::*;

struct First(u64);
impl WireValue for First {
    const SCHEMA_ID: u64 = 11;
    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        Ok(Self(u64::decode(segments)?))
    }
}
impl Action for First {
    const ID: u128 = 41;
    type Output = u64;
}

struct Second(u64);
impl WireValue for Second {
    const SCHEMA_ID: u64 = 12;
    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        Ok(Self(u64::decode(segments)?))
    }
}
impl Action for Second {
    const ID: u128 = 42;
    type Output = u64;
}

struct Collision;
impl WireValue for Collision {
    const SCHEMA_ID: u64 = 13;
    fn encode(self) -> Result<Segments, ActionError> {
        Ok(Vec::new())
    }
    fn decode(_: Segments) -> Result<Self, ActionError> {
        Ok(Self)
    }
}
impl Action for Collision {
    const ID: u128 = First::ID;
    type Output = ();
}

#[test]
fn fingerprint_is_registration_order_independent_and_ids_are_unique() {
    let mut left = ActionRegistry::default();
    left.register::<First, _>(|value| Ok(value.0)).unwrap();
    left.register::<Second, _>(|value| Ok(value.0)).unwrap();
    let mut right = ActionRegistry::default();
    right.register::<Second, _>(|value| Ok(value.0)).unwrap();
    right.register::<First, _>(|value| Ok(value.0)).unwrap();
    assert_eq!(left.fingerprint(), right.fingerprint());
    assert!(matches!(
        left.register::<Collision, _>(|_| Ok(())),
        Err(RuntimeError::DuplicateAction(_))
    ));
    let first = left.get(ActionId::new(First::ID).unwrap()).unwrap();
    assert_eq!(
        first.execute_encoded(First(7).encode().unwrap()),
        Ok(7_u64.encode().unwrap())
    );
}

#[test]
fn erased_dispatch_catches_panics_and_preserves_codec_errors() {
    let mut registry = ActionRegistry::default();
    registry
        .register::<First, _>(|_| -> Result<u64, ActionError> { panic!("boom") })
        .unwrap();
    let handler = registry.get(ActionId::new(First::ID).unwrap()).unwrap();
    assert_eq!(
        handler.execute_encoded(First(1).encode().unwrap()),
        Err(ActionError::Panic)
    );
    assert!(matches!(
        handler.execute_encoded(Vec::new()),
        Err(ActionError::Codec(_))
    ));
}
