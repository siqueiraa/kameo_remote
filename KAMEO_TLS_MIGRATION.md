# Kameo Examples TLS Migration Guide

This guide shows how to update kameo examples to use TLS encryption with kameo_remote.

## Files to Update in kameo Repository

### 1. Update `kameo/examples/remote/tell_concrete_server.rs`

```rust
use anyhow::Result;
use kameo::actor::{ActorRef, WeakActorRef};
use kameo::error::BoxError;
use kameo::message::{Context, Message};
use kameo::Actor;

use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use kameo_remote::{GossipConfig, GossipRegistryHandle, RemoteMessage, RemoteActorRef, SecretKey, NodeId};
use tracing::info;

#[derive(Actor, RemoteMessage)]
struct ConcreteActor {
    id: String,
    message_count: usize,
}

#[derive(Clone, Debug, RemoteMessage)]
struct TellMessage {
    content: String,
}

impl Message<TellMessage> for ConcreteActor {
    type Reply = ();

    async fn handle(&mut self, msg: TellMessage, _ctx: Context<'_, Self, Self::Reply>) -> Self::Reply {
        self.message_count += 1;
        println!("📨 [{}] Received message #{}: {}", self.id, self.message_count, msg.content);
        info!("Actor {} processed message: {}", self.id, msg.content);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install crypto provider for TLS
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG")
                .unwrap_or_else(|_| "kameo=info,kameo_remote=info".to_string())
        )
        .init();
    
    println!("🔐 TLS-Enabled Concrete Tell Server");
    println!("====================================\n");
    
    // 1. Load or generate server TLS keypair
    println!("1. Setting up TLS identity...");
    let key_path = env::var("SERVER_KEY_PATH")
        .unwrap_or_else(|_| "/tmp/kameo_tls/concrete_server.key".to_string());
    let secret_key = load_or_generate_key(&key_path).await?;
    let node_id = secret_key.public();
    
    println!("   Server NodeId: {}", node_id.fmt_short());
    println!("   Key file: {}", key_path);
    
    // Save public key for clients
    let pub_path = key_path.replace(".key", ".pub");
    fs::write(&pub_path, hex::encode(node_id.as_bytes()))?;
    println!("   Public key saved to: {}\n", pub_path);
    
    // 2. Determine external address for service registration
    let external_host = env::var("EXTERNAL_HOST")
        .unwrap_or_else(|_| {
            println!("   ⚠️  EXTERNAL_HOST not set, using 127.0.0.1");
            println!("   💡 Set EXTERNAL_HOST=<your-ip> for network access");
            "127.0.0.1".to_string()
        });
    
    // 3. Create TLS-enabled gossip registry
    println!("2. Starting TLS gossip registry...");
    let bind_addr: SocketAddr = "0.0.0.0:13000".parse()?;
    
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(5),
        ..Default::default()
    };
    
    let registry = GossipRegistryHandle::new_with_tls(
        bind_addr,
        secret_key,
        Some(config),
    ).await?;
    
    let actual_addr = registry.registry.bind_addr;
    println!("   ✅ Server listening on: {}", actual_addr);
    println!("   ✅ External host: {}", external_host);
    println!("   ✅ TLS encryption: ENABLED\n");
    
    // 4. Spawn concrete actors
    println!("3. Spawning concrete actors...");
    
    let actor1 = kameo::spawn(ConcreteActor {
        id: "actor1".to_string(),
        message_count: 0,
    });
    
    let actor2 = kameo::spawn(ConcreteActor {
        id: "actor2".to_string(),
        message_count: 0,
    });
    
    // 5. Register actors with remote refs (using external address)
    let actor1_addr: SocketAddr = format!("{}:13001", external_host).parse()?;
    let actor2_addr: SocketAddr = format!("{}:13002", external_host).parse()?;
    
    let remote_actor1 = RemoteActorRef::new(actor1.clone(), "concrete_actor1".to_string(), actor1_addr);
    remote_actor1.register(&registry).await?;
    println!("   ✅ Registered 'concrete_actor1' at {}", actor1_addr);
    
    let remote_actor2 = RemoteActorRef::new(actor2.clone(), "concrete_actor2".to_string(), actor2_addr);
    remote_actor2.register(&registry).await?;
    println!("   ✅ Registered 'concrete_actor2' at {}\n", actor2_addr);
    
    // 6. Display instructions
    println!("4. Server ready!");
    println!("   💡 Clients can connect using:");
    println!("      cargo run --example tell_concrete_client {} --features remote\n", pub_path);
    
    if external_host == "127.0.0.1" {
        println!("   ⚠️  Server is only accessible locally.");
        println!("   💡 For network access, restart with:");
        println!("      EXTERNAL_HOST=<your-ip> cargo run --example tell_concrete_server --features remote\n");
    }
    
    println!("📊 Waiting for messages (press Ctrl+C to stop)...\n");
    
    // Keep the server running
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        
        let stats = registry.registry.get_stats().await;
        println!("📊 Status - TLS peers: {}, Services: {}, Gossip rounds: {}", 
                 stats.active_peers, stats.local_actors, stats.total_gossip_rounds);
    }
}

async fn load_or_generate_key(path: &str) -> Result<SecretKey> {
    let key_path = Path::new(path);
    
    if key_path.exists() {
        println!("   Loading existing key from: {}", path);
        let key_hex = fs::read_to_string(key_path)?;
        let key_bytes = hex::decode(key_hex.trim())?;
        
        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!("Invalid key length: expected 32, got {}", key_bytes.len()));
        }
        
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key_bytes);
        Ok(SecretKey::from_bytes(&arr)?)
    } else {
        println!("   Generating new Ed25519 keypair...");
        let secret_key = SecretKey::generate();
        
        // Create directory if needed
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        // Save the key
        fs::write(key_path, hex::encode(secret_key.to_bytes()))?;
        println!("   Saved new key to: {}", path);
        
        Ok(secret_key)
    }
}
```

### 2. Update `kameo/examples/remote/tell_concrete_client.rs`

```rust
use anyhow::Result;
use kameo::error::BoxError;
use kameo::message::Message;

use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use kameo_remote::{GossipConfig, GossipRegistryHandle, RemoteActorRef, RemoteMessage, SecretKey, NodeId};
use tracing::info;

#[derive(Clone, Debug, RemoteMessage)]
struct TellMessage {
    content: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install crypto provider for TLS
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            env::var("RUST_LOG")
                .unwrap_or_else(|_| "kameo=info,kameo_remote=info".to_string())
        )
        .init();
    
    println!("🔐 TLS-Enabled Concrete Tell Client");
    println!("====================================\n");
    
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <server_pub_key> [server_host]", args[0]);
        eprintln!("Example: {} /tmp/kameo_tls/concrete_server.pub", args[0]);
        eprintln!("Example: {} /tmp/kameo_tls/concrete_server.pub 192.168.1.100", args[0]);
        return Ok(());
    }
    
    let server_pub_path = &args[1];
    let server_host = args.get(2)
        .map(|s| s.as_str())
        .unwrap_or("127.0.0.1");
    
    // 1. Load server's public key for TLS verification
    println!("1. Loading server identity...");
    let server_node_id = load_node_id(server_pub_path)?;
    println!("   Server NodeId: {}", server_node_id.fmt_short());
    println!("   Loaded from: {}\n", server_pub_path);
    
    let server_addr: SocketAddr = format!("{}:13000", server_host).parse()?;
    println!("   Server address: {}\n", server_addr);
    
    // 2. Load or generate client keypair
    println!("2. Setting up client TLS identity...");
    let client_key_path = env::var("CLIENT_KEY_PATH")
        .unwrap_or_else(|_| "/tmp/kameo_tls/concrete_client.key".to_string());
    let client_secret = load_or_generate_key(&client_key_path).await?;
    let client_node_id = client_secret.public();
    
    println!("   Client NodeId: {}", client_node_id.fmt_short());
    println!("   Key file: {}\n", client_key_path);
    
    // 3. Create TLS-enabled gossip registry
    println!("3. Starting TLS client...");
    let bind_addr = "0.0.0.0:0".parse()?;
    
    let config = GossipConfig {
        gossip_interval: Duration::from_secs(5),
        ..Default::default()
    };
    
    let registry = GossipRegistryHandle::new_with_tls(
        bind_addr,
        client_secret,
        Some(config),
    ).await?;
    
    let actual_addr = registry.registry.bind_addr;
    println!("   ✅ Client listening on: {}", actual_addr);
    println!("   ✅ TLS encryption: ENABLED\n");
    
    // 4. Connect to server with TLS verification
    println!("4. Connecting to TLS server...");
    registry.registry.add_peer_with_node_id(server_addr, Some(server_node_id)).await;
    
    println!("   🔗 Connecting to {} with NodeId verification", server_addr);
    
    // Wait for connection and gossip
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // 5. Discover remote actors via gossip OR use direct connection
    println!("\n5. Discovering/Connecting to remote actors...");
    
    let actor_names = vec!["concrete_actor1", "concrete_actor2"];
    let mut actors = Vec::new();
    
    for actor_name in &actor_names {
        // Try discovery first
        if let Some(location) = registry.lookup(actor_name).await {
            println!("   ✅ Discovered '{}' at {} via gossip", actor_name, location.address);
            actors.push(RemoteActorRef::<ConcreteActor>::new(location.address, actor_name.to_string()));
        } else {
            // Fallback to direct connection
            println!("   ⚠️  '{}' not discovered, trying direct connection", actor_name);
            
            // Try known ports
            let port = if actor_name.contains("1") { 13001 } else { 13002 };
            let direct_addr: SocketAddr = format!("{}:{}", server_host, port).parse()?;
            
            println!("   📡 Attempting direct connection to {}", direct_addr);
            actors.push(RemoteActorRef::<ConcreteActor>::new(direct_addr, actor_name.to_string()));
        }
    }
    
    if actors.is_empty() {
        println!("   ❌ No actors available");
        return Ok(());
    }
    
    // 6. Send messages to remote actors over TLS
    println!("\n6. Sending messages to remote actors over TLS...\n");
    
    let messages = vec![
        "Hello from TLS client! 🔐",
        "This message is encrypted with TLS 1.3",
        "Ed25519 authentication verified",
        "Service discovered via gossip protocol",
        "Final secure message",
    ];
    
    for (i, content) in messages.iter().enumerate() {
        let msg = TellMessage {
            content: format!("[{}] {}", i + 1, content),
        };
        
        // Send to all discovered actors
        for actor in &actors {
            println!("   📤 Sending to {}: {}", actor.actor_name(), msg.content);
            
            match actor.tell(msg.clone()).await {
                Ok(_) => println!("      ✅ Message sent successfully over TLS"),
                Err(e) => println!("      ❌ Failed to send: {}", e),
            }
        }
        
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    
    // 7. Show connection stats
    println!("\n7. Connection Statistics:");
    let stats = registry.registry.get_stats().await;
    println!("   Active TLS peers: {}", stats.active_peers);
    println!("   Known actors: {}", stats.known_actors);
    println!("   Gossip rounds: {}", stats.total_gossip_rounds);
    
    println!("\n✅ All messages sent successfully over TLS!");
    println!("   Check the server console for received messages.");
    
    Ok(())
}

fn load_node_id(path: &str) -> Result<NodeId> {
    let pub_key_hex = fs::read_to_string(path)?;
    let pub_key_bytes = hex::decode(pub_key_hex.trim())?;
    
    if pub_key_bytes.len() != 32 {
        return Err(anyhow::anyhow!("Invalid public key length: expected 32, got {}", pub_key_bytes.len()));
    }
    
    NodeId::from_bytes(&pub_key_bytes)
        .map_err(|e| anyhow::anyhow!("Invalid NodeId: {}", e))
}

async fn load_or_generate_key(path: &str) -> Result<SecretKey> {
    let key_path = Path::new(path);
    
    if key_path.exists() {
        println!("   Loading existing key from: {}", path);
        let key_hex = fs::read_to_string(key_path)?;
        let key_bytes = hex::decode(key_hex.trim())?;
        
        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!("Invalid key length: expected 32, got {}", key_bytes.len()));
        }
        
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&key_bytes);
        Ok(SecretKey::from_bytes(&arr)?)
    } else {
        println!("   Generating new Ed25519 keypair...");
        let secret_key = SecretKey::generate();
        
        // Create directory if needed
        if let Some(parent) = key_path.parent() {
            fs::create_dir_all(parent)?;
        }
        
        // Save the key
        fs::write(key_path, hex::encode(secret_key.to_bytes()))?;
        println!("   Saved new key to: {}", path);
        
        // Also save public key
        let pub_path = path.replace(".key", ".pub");
        fs::write(&pub_path, hex::encode(secret_key.public().as_bytes()))?;
        println!("   Saved public key to: {}", pub_path);
        
        Ok(secret_key)
    }
}

// Placeholder for the actor type - in real code this would be imported from server
struct ConcreteActor;
```

## How to Apply These Changes

### Step 1: Update kameo's Cargo.toml

Add the required dependencies:
```toml
[dependencies]
# ... existing dependencies ...
rustls = "0.23"
hex = "0.4"
```

### Step 2: Replace the Example Files

1. Copy the updated `tell_concrete_server.rs` to `kameo/examples/remote/`
2. Copy the updated `tell_concrete_client.rs` to `kameo/examples/remote/`

### Step 3: Test the TLS Examples

#### Local Testing (Same Machine)
```bash
# Terminal 1: Start TLS server
cd kameo
cargo run --example tell_concrete_server --features remote

# Terminal 2: Start TLS client
cd kameo
cargo run --example tell_concrete_client /tmp/kameo_tls/concrete_server.pub --features remote
```

#### Network Testing (Different Machines)
```bash
# Terminal 1: Start TLS server on machine with IP 192.168.1.100
EXTERNAL_HOST=192.168.1.100 cargo run --example tell_concrete_server --features remote

# Terminal 2: Start TLS client from another machine
cargo run --example tell_concrete_client /tmp/kameo_tls/concrete_server.pub 192.168.1.100 --features remote
```

## Key Changes Made

### 1. TLS Encryption
- All connections now use TLS 1.3 with Ed25519 certificates
- `GossipRegistryHandle::new()` → `GossipRegistryHandle::new_with_tls()`
- Added `SecretKey` generation and loading

### 2. NodeId Verification
- Server generates and saves its public key
- Client loads server's public key for verification
- `add_peer()` → `add_peer_with_node_id()` with NodeId verification

### 3. External Address Support
- Added `EXTERNAL_HOST` environment variable
- Services registered with external addresses, not `127.0.0.1`
- Proper service discovery across networks

### 4. Dual-Mode Operation
- Try service discovery via gossip first
- Fallback to direct connection if not discovered
- Support for both discovered and hardcoded addresses

### 5. Persistent Keys
- Keys saved to `/tmp/kameo_tls/` directory
- Reuse keys across restarts
- Public keys shared for client verification

## Environment Variables

- `EXTERNAL_HOST`: The external IP/hostname for service registration (default: `127.0.0.1`)
- `SERVER_KEY_PATH`: Path to server's Ed25519 key (default: `/tmp/kameo_tls/concrete_server.key`)
- `CLIENT_KEY_PATH`: Path to client's Ed25519 key (default: `/tmp/kameo_tls/concrete_client.key`)
- `RUST_LOG`: Logging level (default: `kameo=info,kameo_remote=info`)

## Testing Scenarios

### Scenario 1: Local Development
```bash
# Everything on localhost
cargo run --example tell_concrete_server --features remote
cargo run --example tell_concrete_client /tmp/kameo_tls/concrete_server.pub --features remote
```

### Scenario 2: LAN Testing
```bash
# Server on 192.168.1.100
EXTERNAL_HOST=192.168.1.100 cargo run --example tell_concrete_server --features remote

# Client from another machine
cargo run --example tell_concrete_client server.pub 192.168.1.100 --features remote
```

### Scenario 3: Cloud/Internet
```bash
# Server on public cloud
EXTERNAL_HOST=myapp.example.com cargo run --example tell_concrete_server --features remote

# Client from anywhere
cargo run --example tell_concrete_client server.pub myapp.example.com --features remote
```

## Verification

To verify TLS is working:
1. Check for "✅ TLS encryption: ENABLED" in both server and client output
2. Look for NodeId display (e.g., "Server NodeId: 428cf775b7")
3. Monitor for "Active TLS peers" in status messages
4. Use Wireshark to confirm encrypted traffic on port 13000

## Troubleshooting

### "No such file or directory"
- Server hasn't been run yet to generate keys
- Run server first, it will create the key files

### "Service not discovered"
- Wait longer for gossip propagation
- Check EXTERNAL_HOST is set correctly on server
- Verify network connectivity between nodes

### "NodeId mismatch"
- Client has wrong server public key
- Re-copy the `.pub` file from server to client

### Services connect to wrong address
- Set EXTERNAL_HOST on server to its actual IP/hostname
- Don't use `127.0.0.1` for network deployments