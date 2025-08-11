# Service Discovery Address Fix

## Problem

Currently, service registration is broken for real-world networking:

```rust
// Server binds to:
let bind_addr = "0.0.0.0:29001";  // or 127.0.0.1:29001

// Server registers services as:
registry.register("chat_service", "127.0.0.1:39001").await;

// Client connects to server at:
"192.168.1.100:29001"  // Server's actual IP

// Client discovers service as:
"127.0.0.1:39001"  // ❌ WRONG! This is localhost on the client!
```

The client will try to connect to its own localhost, not the server's services.

## Dual-Mode Operation: Direct Connection vs Service Discovery

The system MUST support two modes of operation:

### Mode 1: Direct IP/Port Connection (No Discovery)
```rust
// Client knows the exact address of the service
let service_addr = "192.168.1.100:39001";
let actor_ref = RemoteActorRef::new(service_addr, "chat_actor");
actor_ref.tell(msg).await?;  // Direct connection to IP:port
```

### Mode 2: Service Discovery (Via Gossip)
```rust
// Client only knows the service name, not the address
if let Some(location) = registry.lookup("chat_service").await {
    // Found via gossip - use discovered address
    let actor_ref = RemoteActorRef::new(location.address, "chat_actor");
    actor_ref.tell(msg).await?;
} else {
    // Not found - must use direct connection or wait
    println!("Service not discovered yet");
}
```

### How Clients Decide: Discovery vs Direct

```rust
// Hybrid approach - try discovery first, fallback to direct
async fn connect_to_service(
    registry: &GossipRegistryHandle,
    service_name: &str,
    fallback_addr: Option<SocketAddr>,
) -> Result<RemoteActorRef> {
    // First: Try service discovery
    if let Some(location) = registry.lookup(service_name).await {
        println!("✅ Found {} via gossip at {}", service_name, location.address);
        return Ok(RemoteActorRef::new(location.address, service_name));
    }
    
    // Second: Use fallback address if provided
    if let Some(addr) = fallback_addr {
        println!("⚠️ {} not discovered, using direct connection to {}", service_name, addr);
        return Ok(RemoteActorRef::new(addr, service_name));
    }
    
    // Third: Wait and retry discovery
    println!("⏳ Waiting for {} to be discovered...", service_name);
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Some(location) = registry.lookup(service_name).await {
            println!("✅ Found {} via gossip at {}", service_name, location.address);
            return Ok(RemoteActorRef::new(location.address, service_name));
        }
    }
    
    Err("Service not found and no fallback address provided")
}
```

## When to Use Each Mode

### Use Direct IP/Port When:
- ✅ You know the exact service address
- ✅ The service is on a different network (no gossip connectivity)
- ✅ You need immediate connection (no discovery delay)
- ✅ Testing or debugging specific endpoints
- ✅ Connecting to legacy services without gossip

### Use Service Discovery When:
- ✅ Services may move between nodes
- ✅ You want automatic failover
- ✅ Services are dynamically deployed
- ✅ Multiple instances of the same service exist
- ✅ You don't know the service address in advance

### Real-World Example

```rust
// Server Side - Registers services with correct external addresses
let external_host = std::env::var("EXTERNAL_HOST").unwrap_or("127.0.0.1".to_string());

// Register with external address for discovery
registry.register("chat_service", format!("{}:39001", external_host).parse()?).await?;
registry.register("auth_service", format!("{}:39002", external_host).parse()?).await?;

// Client Side - Smart connection logic
async fn connect_to_chat(registry: &GossipRegistryHandle) -> Result<RemoteActorRef> {
    // Try discovery first (preferred)
    if let Some(location) = registry.lookup("chat_service").await {
        info!("Found chat_service via discovery at {}", location.address);
        return Ok(RemoteActorRef::new(location.address, "chat"));
    }
    
    // Fallback to known addresses (for specific deployments)
    let fallback_addrs = [
        "prod-chat-1.example.com:39001",
        "prod-chat-2.example.com:39001",
        "192.168.1.100:39001",  // LAN fallback
    ];
    
    for addr in &fallback_addrs {
        if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
            match test_connection(socket_addr).await {
                Ok(_) => {
                    warn!("Using fallback address {} for chat_service", addr);
                    return Ok(RemoteActorRef::new(socket_addr, "chat"));
                }
                Err(_) => continue,
            }
        }
    }
    
    Err("Chat service not available via discovery or fallback addresses")
}
```

## Root Cause

The issue is that we're conflating several different addresses:

1. **Bind Address** (`0.0.0.0:29001`) - Where the server listens locally
2. **External Address** (`192.168.1.100:29001`) - How clients reach the server  
3. **Service Addresses** (`192.168.1.100:39001`) - How clients reach services

Currently, we have no way to specify the external address that should be used for service discovery.

## Solution Options

### Option 1: Explicit External Address (Recommended)

Add an `external_address` parameter to the registry:

```rust
// Server specifies its external address
let registry = GossipRegistryHandle::new_with_tls_and_external(
    bind_addr: "0.0.0.0:29001",        // Where to bind
    external_addr: "192.168.1.100",     // Our public IP/hostname
    secret_key,
    config,
).await?;

// When registering services, use the external address
registry.register("chat_service", 39001).await;  // Just port number
// Internally becomes: "192.168.1.100:39001"
```

### Option 2: Detect from Incoming Connections

When a client connects, detect what address they used:

```rust
// In handle_connection:
let peer_view_of_us = stream.local_addr()?;  // What address did they connect to?
// Use this for service registration
```

Problems:
- Doesn't work until first connection
- Different clients might see different addresses (NAT, multiple IPs)
- Race conditions

### Option 3: Configuration File

```toml
[network]
bind_address = "0.0.0.0:29001"
external_address = "myserver.example.com"
service_ports = [39001, 39002, 39003]
```

### Option 4: Smart Registration

Make registration aware of the context:

```rust
// Register with explicit external address
registry.register_external("chat_service", "myserver.example.com:39001").await;

// Or register relative to our external address
registry.register_relative("chat_service", 39001).await;
```

## Recommended Implementation Plan

### Phase 1: Add External Address Support

1. **Update GossipRegistry**:
```rust
pub struct GossipRegistry {
    pub bind_addr: SocketAddr,        // Where we listen
    pub external_addr: Option<String>, // How others reach us (IP or hostname)
    // ...
}
```

2. **Update GossipRegistryHandle::new_with_tls**:
```rust
pub async fn new_with_tls(
    bind_addr: SocketAddr,
    external_addr: Option<String>,  // NEW: External address
    secret_key: SecretKey,
    config: Option<GossipConfig>,
) -> Result<Self>
```

3. **Smart Registration Methods**:
```rust
impl GossipRegistryHandle {
    /// Register using just a port (uses external_addr as base)
    pub async fn register_port(&self, name: String, port: u16) -> Result<()> {
        let addr = match &self.registry.external_addr {
            Some(host) => format!("{}:{}", host, port),
            None => {
                // Fallback to bind address (will break for 0.0.0.0)
                warn!("No external address configured, using bind address");
                format!("{}:{}", self.registry.bind_addr.ip(), port)
            }
        };
        self.register(name, addr.parse()?).await
    }
    
    /// Register with explicit full address (for complex setups)
    pub async fn register_full(&self, name: String, addr: SocketAddr) -> Result<()> {
        self.register(name, addr).await
    }
}
```

### Phase 2: Update Examples

```rust
// Server knows its external address
let external_addr = std::env::var("EXTERNAL_ADDR")
    .unwrap_or_else(|_| "127.0.0.1".to_string());

let registry = GossipRegistryHandle::new_with_tls(
    "0.0.0.0:29001".parse()?,
    Some(external_addr.clone()),
    secret_key,
    None,
).await?;

// Register services using just ports
registry.register_port("chat_service", 39001).await?;
registry.register_port("auth_service", 39002).await?;
```

### Phase 3: Environment Variable Support

```bash
# Run server with external address
EXTERNAL_ADDR=192.168.1.100 cargo run --example tls_server

# Or with hostname
EXTERNAL_ADDR=myserver.example.com cargo run --example tls_server
```

### Phase 4: Documentation

Update all examples to show:
1. Local testing (127.0.0.1)
2. LAN testing (192.168.x.x)
3. Internet testing (public IP or domain)
4. NAT/Docker scenarios

## Complete Flow Examples

### Example 1: Service Discovery Success Flow

```rust
// ==== SERVER SIDE ====
// Server at 192.168.1.100
let external_host = "192.168.1.100";
let registry = GossipRegistryHandle::new_with_tls(
    "0.0.0.0:29001".parse()?,  // Bind to all interfaces
    secret_key,
    None,
).await?;

// Register services with external address
registry.register("chat_service", format!("{}:39001", external_host).parse()?).await?;
println!("Registered chat_service at {}:39001", external_host);

// ==== CLIENT SIDE ====
// Connect to gossip network
let registry = GossipRegistryHandle::new_with_tls(
    "0.0.0.0:0".parse()?,  // Random port
    client_secret_key,
    None,
).await?;

// Add server as gossip peer
registry.add_peer_with_node_id("192.168.1.100:29001".parse()?, server_node_id).await;

// Wait for gossip propagation
tokio::time::sleep(Duration::from_secs(2)).await;

// Try to discover service
match registry.lookup("chat_service").await {
    Some(location) => {
        println!("✅ Discovered chat_service at {}", location.address);
        // location.address = "192.168.1.100:39001" (correct!)
        let actor = RemoteActorRef::new(location.address, "chat");
        actor.tell("Hello").await?;
    }
    None => {
        println!("❌ Service not discovered");
    }
}
```

### Example 2: Direct Connection Fallback Flow

```rust
// ==== CLIENT SIDE (No Gossip) ====
// Client doesn't participate in gossip, uses direct connection

async fn connect_to_service() -> Result<RemoteActorRef> {
    // No registry/gossip available
    let known_addresses = vec![
        "192.168.1.100:39001",  // Primary
        "192.168.1.101:39001",  // Backup
    ];
    
    for addr_str in known_addresses {
        let addr: SocketAddr = addr_str.parse()?;
        
        // Try direct connection
        match TcpStream::connect(addr).await {
            Ok(_) => {
                println!("✅ Connected directly to {}", addr);
                return Ok(RemoteActorRef::new(addr, "chat"));
            }
            Err(e) => {
                println!("❌ Failed to connect to {}: {}", addr, e);
                continue;
            }
        }
    }
    
    Err("No services available")
}
```

### Example 3: Hybrid Discovery + Direct

```rust
// ==== CLIENT SIDE (Smart Client) ====
pub struct SmartServiceConnector {
    registry: Option<GossipRegistryHandle>,
    known_addresses: HashMap<String, Vec<SocketAddr>>,
}

impl SmartServiceConnector {
    pub async fn connect(&self, service_name: &str) -> Result<RemoteActorRef> {
        // Step 1: Try gossip discovery (if available)
        if let Some(registry) = &self.registry {
            if let Some(location) = registry.lookup(service_name).await {
                info!("Found {} via gossip at {}", service_name, location.address);
                return Ok(RemoteActorRef::new(location.address, service_name));
            }
        }
        
        // Step 2: Try known addresses
        if let Some(addrs) = self.known_addresses.get(service_name) {
            for addr in addrs {
                if self.test_connection(*addr).await.is_ok() {
                    warn!("Using known address {} for {}", addr, service_name);
                    return Ok(RemoteActorRef::new(*addr, service_name));
                }
            }
        }
        
        // Step 3: Try discovery again with wait
        if let Some(registry) = &self.registry {
            info!("Waiting for {} discovery...", service_name);
            for i in 1..=5 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Some(location) = registry.lookup(service_name).await {
                    info!("Found {} via gossip after {} attempts", service_name, i);
                    return Ok(RemoteActorRef::new(location.address, service_name));
                }
            }
        }
        
        Err(format!("Service {} not available", service_name))
    }
}

// Usage
let connector = SmartServiceConnector {
    registry: Some(registry),
    known_addresses: HashMap::from([
        ("chat_service".to_string(), vec!["192.168.1.100:39001".parse()?]),
        ("auth_service".to_string(), vec!["192.168.1.100:39002".parse()?]),
    ]),
};

let chat = connector.connect("chat_service").await?;
```

## Testing Scenarios

### Scenario 1: Local Development
```bash
# Everything on localhost - works as-is
EXTERNAL_HOST=127.0.0.1 cargo run --example server
cargo run --example client  # Will discover via gossip
```

### Scenario 2: LAN Testing
```bash
# Server on 192.168.1.100
EXTERNAL_HOST=192.168.1.100 cargo run --example server

# Client from another machine
cargo run --example client  # Connects to 192.168.1.100:29001 for gossip
```

### Scenario 3: Internet/Cloud
```bash
# Server on AWS/GCP/etc
EXTERNAL_HOST=myapp.example.com cargo run --example server

# Client from anywhere
cargo run --example client myapp.example.com  # Discovers services via gossip
```

### Scenario 4: Docker/Kubernetes
```yaml
env:
  - name: EXTERNAL_HOST
    value: "my-service.default.svc.cluster.local"
```

### Scenario 5: Mixed Environment (Some with Gossip, Some Without)
```rust
// Services that participate in gossip
let gossip_services = registry.lookup("chat_service").await;

// Legacy services with direct connection only
let legacy_chat = RemoteActorRef::new("legacy-chat.internal:8080".parse()?, "chat");

// External third-party services
let external_api = RemoteActorRef::new("api.external.com:443".parse()?, "api");
```

## Migration Path

1. **Backward Compatible**: Keep existing `register(name, addr)` method
2. **Add New Methods**: `register_port(name, port)` for external-aware registration
3. **Deprecation**: Eventually deprecate hardcoded addresses in examples
4. **Smart Defaults**: If no external_addr, try to detect from first connection

## Implementation Priority

1. ✅ High: Fix examples to not use hardcoded `127.0.0.1` addresses
2. ✅ High: Add external_addr support to GossipRegistry
3. ✅ Medium: Add register_port() convenience method
4. ⬜ Low: Auto-detection from connections
5. ⬜ Low: Configuration file support

## Example Fix

### Before (Broken):
```rust
// Server registers with hardcoded localhost
registry.register("chat", "127.0.0.1:39001".parse()?).await?;

// Client discovers "127.0.0.1:39001" - tries to connect to itself!
```

### After (Fixed):
```rust
// Server knows its external address
let registry = GossipRegistryHandle::new_with_tls(
    "0.0.0.0:29001".parse()?,
    Some("192.168.1.100".to_string()),
    secret_key,
    None,
).await?;

// Register using port only
registry.register_port("chat", 39001).await?;

// Client discovers "192.168.1.100:39001" - correct!
```

## Next Steps

1. Implement external_addr support in GossipRegistry
2. Add register_port() method
3. Update all examples to use external addresses
4. Test in Docker/NAT environments
5. Document best practices for different deployment scenarios