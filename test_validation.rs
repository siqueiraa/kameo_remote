use kameo_remote::PeerId;

fn main() {
    // Test all-zero key
    let zero_peer_id = PeerId::from_public_key_bytes([0u8; 32]);
    println!("Zero PeerId: {}", zero_peer_id);
    
    match zero_peer_id.verifying_key() {
        Ok(_) => println!("❌ All-zero key is considered VALID by Ed25519"),
        Err(e) => println!("✅ All-zero key rejected: {}", e),
    }
    
    // Test invalid key (not on curve)
    let invalid_bytes = [255u8; 32]; // All 0xFF - not a valid Ed25519 point
    let invalid_peer_id = PeerId::from_public_key_bytes(invalid_bytes);
    println!("Invalid PeerId: {}", invalid_peer_id);
    
    match invalid_peer_id.verifying_key() {
        Ok(_) => println!("❌ Invalid key is considered VALID by Ed25519"),
        Err(e) => println!("✅ Invalid key rejected: {}", e),
    }
}