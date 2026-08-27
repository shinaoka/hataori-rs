use super::{connect_until, io_error};
use crate::{
    protocol::{LocalityId, RunId},
    transport::TransportError,
};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    str::FromStr,
    thread,
    time::{Duration, Instant},
};

const JOIN_MAGIC: [u8; 4] = *b"HTRJ";
const MEMBERSHIP_MAGIC: [u8; 4] = *b"HTRM";
const MAX_ENDPOINT_BYTES: usize = 128;
const MAX_LOCALITIES: usize = 1024;

#[derive(Clone, Debug)]
pub struct TcpRendezvousConfig {
    pub run_id: RunId,
    pub rendezvous_endpoint: SocketAddr,
    pub local_endpoint: SocketAddr,
    pub expected_localities: usize,
    pub coordinator: bool,
    pub timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TcpMembership {
    pub local_id: LocalityId,
    pub endpoints: Vec<SocketAddr>,
}

#[derive(Debug)]
pub struct TcpRendezvous;

impl TcpRendezvous {
    pub fn join(config: TcpRendezvousConfig) -> Result<TcpMembership, TransportError> {
        validate_config(&config)?;
        if config.coordinator {
            coordinate(config)
        } else {
            join_coordinator(config)
        }
    }
}

fn validate_config(config: &TcpRendezvousConfig) -> Result<(), TransportError> {
    if config.expected_localities == 0 || config.expected_localities > MAX_LOCALITIES {
        return Err(TransportError::Peer(
            "TCP rendezvous locality count is out of bounds".into(),
        ));
    }
    if config.timeout.is_zero() {
        return Err(TransportError::Timeout("TCP rendezvous configuration"));
    }
    Ok(())
}

fn coordinate(config: TcpRendezvousConfig) -> Result<TcpMembership, TransportError> {
    let listener = TcpListener::bind(config.rendezvous_endpoint).map_err(io_error)?;
    listener.set_nonblocking(true).map_err(io_error)?;
    let deadline = Instant::now() + config.timeout;
    let mut endpoints = vec![config.local_endpoint];
    let mut clients: Vec<TcpStream> = Vec::new();
    while endpoints.len() < config.expected_localities {
        if Instant::now() >= deadline {
            return Err(TransportError::Timeout("TCP rendezvous accept"));
        }
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream
                    .set_read_timeout(Some(config.timeout))
                    .map_err(io_error)?;
                stream
                    .set_write_timeout(Some(config.timeout))
                    .map_err(io_error)?;
                let (run_id, endpoint) = read_join(&mut stream)?;
                if run_id != config.run_id {
                    return Err(TransportError::Protocol(
                        crate::protocol::ProtocolError::RunMismatch,
                    ));
                }
                if endpoints.contains(&endpoint) {
                    return Err(TransportError::Peer(
                        "TCP rendezvous endpoint is duplicated".into(),
                    ));
                }
                endpoints.push(endpoint);
                clients.push(stream);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(io_error(error)),
        }
    }
    for (offset, stream) in clients.iter_mut().enumerate() {
        write_membership(
            stream,
            config.run_id,
            LocalityId::new((offset + 1) as u64),
            &endpoints,
        )?;
    }
    Ok(TcpMembership {
        local_id: LocalityId::new(0),
        endpoints,
    })
}

fn join_coordinator(config: TcpRendezvousConfig) -> Result<TcpMembership, TransportError> {
    let deadline = Instant::now() + config.timeout;
    let mut stream = connect_until(config.rendezvous_endpoint, deadline)?;
    stream
        .set_read_timeout(Some(config.timeout))
        .map_err(io_error)?;
    stream
        .set_write_timeout(Some(config.timeout))
        .map_err(io_error)?;
    write_join(&mut stream, config.run_id, config.local_endpoint)?;
    let membership = read_membership(&mut stream, config.run_id)?;
    if membership.endpoints.len() != config.expected_localities {
        return Err(TransportError::Peer(
            "TCP rendezvous membership size mismatch".into(),
        ));
    }
    let index = membership.local_id.get() as usize;
    if membership.endpoints.get(index) != Some(&config.local_endpoint) {
        return Err(TransportError::Peer(
            "TCP rendezvous assigned endpoint mismatch".into(),
        ));
    }
    Ok(membership)
}

fn write_join(
    stream: &mut TcpStream,
    run_id: RunId,
    endpoint: SocketAddr,
) -> Result<(), TransportError> {
    let endpoint = endpoint.to_string();
    if endpoint.len() > MAX_ENDPOINT_BYTES {
        return Err(TransportError::ByteLimit {
            actual: endpoint.len(),
            limit: MAX_ENDPOINT_BYTES,
        });
    }
    let length: u16 = endpoint
        .len()
        .try_into()
        .map_err(|_| TransportError::Peer("endpoint length overflow".into()))?;
    let mut frame = Vec::with_capacity(22 + endpoint.len());
    frame.extend_from_slice(&JOIN_MAGIC);
    frame.extend_from_slice(&run_id.get().to_le_bytes());
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(endpoint.as_bytes());
    stream.write_all(&frame).map_err(io_error)
}

fn read_join(stream: &mut TcpStream) -> Result<(RunId, SocketAddr), TransportError> {
    let mut header = [0_u8; 22];
    stream.read_exact(&mut header).map_err(io_error)?;
    if header[..4] != JOIN_MAGIC {
        return Err(TransportError::Peer(
            "TCP rendezvous join magic mismatch".into(),
        ));
    }
    let run_id = RunId::new(u128::from_le_bytes(header[4..20].try_into().unwrap()))?;
    let length = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    if length == 0 || length > MAX_ENDPOINT_BYTES {
        return Err(TransportError::ByteLimit {
            actual: length,
            limit: MAX_ENDPOINT_BYTES,
        });
    }
    let mut endpoint = vec![0_u8; length];
    stream.read_exact(&mut endpoint).map_err(io_error)?;
    let endpoint =
        std::str::from_utf8(&endpoint).map_err(|error| TransportError::Peer(error.to_string()))?;
    Ok((
        run_id,
        SocketAddr::from_str(endpoint).map_err(|error| TransportError::Peer(error.to_string()))?,
    ))
}

fn write_membership(
    stream: &mut TcpStream,
    run_id: RunId,
    local_id: LocalityId,
    endpoints: &[SocketAddr],
) -> Result<(), TransportError> {
    let count: u32 = endpoints
        .len()
        .try_into()
        .map_err(|_| TransportError::Peer("membership count overflow".into()))?;
    let mut frame = Vec::new();
    frame.extend_from_slice(&MEMBERSHIP_MAGIC);
    frame.extend_from_slice(&run_id.get().to_le_bytes());
    frame.extend_from_slice(&local_id.get().to_le_bytes());
    frame.extend_from_slice(&count.to_le_bytes());
    for endpoint in endpoints {
        let endpoint = endpoint.to_string();
        if endpoint.len() > MAX_ENDPOINT_BYTES {
            return Err(TransportError::ByteLimit {
                actual: endpoint.len(),
                limit: MAX_ENDPOINT_BYTES,
            });
        }
        let length: u16 = endpoint
            .len()
            .try_into()
            .map_err(|_| TransportError::Peer("endpoint length overflow".into()))?;
        frame.extend_from_slice(&length.to_le_bytes());
        frame.extend_from_slice(endpoint.as_bytes());
    }
    stream.write_all(&frame).map_err(io_error)
}

fn read_membership(
    stream: &mut TcpStream,
    expected_run: RunId,
) -> Result<TcpMembership, TransportError> {
    let mut header = [0_u8; 32];
    stream.read_exact(&mut header).map_err(io_error)?;
    if header[..4] != MEMBERSHIP_MAGIC {
        return Err(TransportError::Peer(
            "TCP rendezvous membership magic mismatch".into(),
        ));
    }
    let run_id = RunId::new(u128::from_le_bytes(header[4..20].try_into().unwrap()))?;
    if run_id != expected_run {
        return Err(TransportError::Protocol(
            crate::protocol::ProtocolError::RunMismatch,
        ));
    }
    let local_id = LocalityId::new(u64::from_le_bytes(header[20..28].try_into().unwrap()));
    let count = u32::from_le_bytes(header[28..32].try_into().unwrap()) as usize;
    if count == 0 || count > MAX_LOCALITIES || local_id.get() as usize >= count {
        return Err(TransportError::Peer(
            "TCP rendezvous membership metadata is invalid".into(),
        ));
    }
    let mut endpoints = Vec::with_capacity(count);
    for _ in 0..count {
        let mut length = [0_u8; 2];
        stream.read_exact(&mut length).map_err(io_error)?;
        let length = u16::from_le_bytes(length) as usize;
        if length == 0 || length > MAX_ENDPOINT_BYTES {
            return Err(TransportError::ByteLimit {
                actual: length,
                limit: MAX_ENDPOINT_BYTES,
            });
        }
        let mut endpoint = vec![0_u8; length];
        stream.read_exact(&mut endpoint).map_err(io_error)?;
        let endpoint = std::str::from_utf8(&endpoint)
            .map_err(|error| TransportError::Peer(error.to_string()))?;
        endpoints.push(
            SocketAddr::from_str(endpoint)
                .map_err(|error| TransportError::Peer(error.to_string()))?,
        );
    }
    let mut unique = endpoints.clone();
    unique.sort_unstable();
    unique.dedup();
    if unique.len() != endpoints.len() {
        return Err(TransportError::Peer(
            "TCP rendezvous membership contains duplicate endpoints".into(),
        ));
    }
    Ok(TcpMembership {
        local_id,
        endpoints,
    })
}
