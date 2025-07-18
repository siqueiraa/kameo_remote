use crate::{current_timestamp, RegistrationPriority};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::net::SocketAddr;

/// Location of a remote actor - just the address since this is remote-only
/// For remote actors, we just need to know their advertised address
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq)]
pub struct ActorLocation {
    pub address: String,                 // Use String instead of SocketAddr for rkyv compatibility
    pub wall_clock_time: u64,           // Still needed for TTL calculations
    pub priority: RegistrationPriority, // Registration priority for propagation
    pub local_registration_time: u128,  // Precise registration time for timing measurements
}

impl ActorLocation {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address: address.to_string(),
            wall_clock_time: current_timestamp(),
            priority: RegistrationPriority::Normal,
            local_registration_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        }
    }

    /// Create with specific priority
    pub fn new_with_priority(address: SocketAddr, priority: RegistrationPriority) -> Self {
        Self {
            address: address.to_string(),
            wall_clock_time: current_timestamp(),
            priority,
            local_registration_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        }
    }

    /// Create with current wall clock time
    pub fn new_with_wall_time(address: SocketAddr, wall_time: u64) -> Self {
        Self {
            address: address.to_string(),
            wall_clock_time: wall_time,
            priority: RegistrationPriority::Normal,
            local_registration_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        }
    }

    /// Create with both wall time and priority
    pub fn new_with_wall_time_and_priority(
        address: SocketAddr,
        wall_time: u64,
        priority: RegistrationPriority,
    ) -> Self {
        Self {
            address: address.to_string(),
            wall_clock_time: wall_time,
            priority,
            local_registration_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        }
    }
    
    /// Get the socket address as a SocketAddr (for compatibility)
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        self.address.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_location_new() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let location = ActorLocation::new(addr);

        assert_eq!(location.address, addr.to_string());
        assert_eq!(location.priority, RegistrationPriority::Normal);
        assert!(location.wall_clock_time > 0);
        assert!(location.local_registration_time > 0);
    }

    #[test]
    fn test_actor_location_new_with_priority() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let location = ActorLocation::new_with_priority(addr, RegistrationPriority::Immediate);

        assert_eq!(location.address, addr.to_string());
        assert_eq!(location.priority, RegistrationPriority::Immediate);
        assert!(location.wall_clock_time > 0);
        assert!(location.local_registration_time > 0);
    }

    #[test]
    fn test_actor_location_new_with_wall_time() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let wall_time = 12345678;
        let location = ActorLocation::new_with_wall_time(addr, wall_time);

        assert_eq!(location.address, addr.to_string());
        assert_eq!(location.wall_clock_time, wall_time);
        assert_eq!(location.priority, RegistrationPriority::Normal);
        assert!(location.local_registration_time > 0);
    }

    #[test]
    fn test_actor_location_new_with_wall_time_and_priority() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let wall_time = 12345678;
        let location = ActorLocation::new_with_wall_time_and_priority(
            addr,
            wall_time,
            RegistrationPriority::Immediate,
        );

        assert_eq!(location.address, addr.to_string());
        assert_eq!(location.wall_clock_time, wall_time);
        assert_eq!(location.priority, RegistrationPriority::Immediate);
        assert!(location.local_registration_time > 0);
    }

    #[test]
    fn test_actor_location_clone() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let location = ActorLocation::new_with_priority(addr, RegistrationPriority::Immediate);
        let cloned = location.clone();

        assert_eq!(location.address, cloned.address);
        assert_eq!(location.wall_clock_time, cloned.wall_clock_time);
        assert_eq!(location.priority, cloned.priority);
        assert_eq!(
            location.local_registration_time,
            cloned.local_registration_time
        );
    }

    #[test]
    fn test_actor_location_equality() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Create with specific values to test equality
        let location1 = ActorLocation {
            address: addr.to_string(),
            wall_clock_time: 1000,
            priority: RegistrationPriority::Normal,
            local_registration_time: 1000,
        };
        let location2 = ActorLocation {
            address: addr.to_string(),
            wall_clock_time: 1000,
            priority: RegistrationPriority::Normal,
            local_registration_time: 1000,
        };
        assert_eq!(location1, location2);

        // Different timestamps should make them unequal
        let location3 = ActorLocation {
            address: addr.to_string(),
            wall_clock_time: 1001,
            priority: RegistrationPriority::Normal,
            local_registration_time: 1000,
        };
        assert_ne!(location1, location3);

        // Different address should make them unequal
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let location4 = ActorLocation {
            address: addr2.to_string(),
            wall_clock_time: 1000,
            priority: RegistrationPriority::Normal,
            local_registration_time: 1000,
        };
        assert_ne!(location1, location4);
    }

    #[test]
    fn test_actor_location_debug() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let location = ActorLocation::new(addr);
        let debug_str = format!("{:?}", location);

        assert!(debug_str.contains("ActorLocation"));
        assert!(debug_str.contains("127.0.0.1:8080"));
        assert!(debug_str.contains("priority"));
    }

    #[test]
    fn test_actor_location_serialization() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let location = ActorLocation::new_with_priority(addr, RegistrationPriority::Immediate);

        let serialized = rkyv::to_bytes::<rkyv::rancor::Error>(&location).unwrap();
        let deserialized: ActorLocation = rkyv::from_bytes::<ActorLocation, rkyv::rancor::Error>(&serialized).unwrap();

        assert_eq!(location.address, deserialized.address);
        assert_eq!(location.wall_clock_time, deserialized.wall_clock_time);
        assert_eq!(location.priority, deserialized.priority);
        assert_eq!(
            location.local_registration_time,
            deserialized.local_registration_time
        );
    }
}
