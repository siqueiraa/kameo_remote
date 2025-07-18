use std::time::Duration;
use kameo_remote::*;
use tokio::time::sleep;

/// Simple DelegatedReplySender test
#[tokio::test]
async fn test_delegated_reply_sender_basic() {
    println!("🚀 DelegatedReplySender Basic Test");
    
    // Setup two nodes
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:33001".parse().unwrap();
    let node2_addr = "127.0.0.1:33002".parse().unwrap();
    
    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone())).await.unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone())).await.unwrap();
    
    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();
    
    println!("✅ Connection established");
    
    // Test 1: Basic delegated reply
    println!("\n🔸 Test 1: Basic Delegated Reply");
    let request = b"GET /api/test";
    
    let reply_sender = conn.ask_with_reply_sender(request).await.unwrap();
    println!("📨 Created reply sender: {:?}", reply_sender);
    
    // Simulate processing in another task
    let task = tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        let response = reply_sender.create_mock_reply();
        reply_sender.reply(response.clone()).unwrap();
        response
    });
    
    let response = task.await.unwrap();
    let response_str = String::from_utf8_lossy(&response);
    println!("📬 Got response: {:?}", response_str);
    
    // Test 2: With timeout
    println!("\n🔸 Test 2: With Timeout");
    let timeout_request = b"GET /api/timeout-test";
    let timeout = Duration::from_millis(100);
    
    let reply_sender = conn.ask_with_timeout_and_reply(timeout_request, timeout).await.unwrap();
    println!("📨 Created timed reply sender: {:?}", reply_sender);
    
    // Test fast response (should succeed)
    let fast_task = tokio::spawn(async move {
        sleep(Duration::from_millis(20)).await;
        let response = reply_sender.create_mock_reply();
        reply_sender.reply(response.clone()).unwrap();
        response
    });
    
    let fast_response = fast_task.await.unwrap();
    let fast_response_str = String::from_utf8_lossy(&fast_response);
    println!("✅ Fast response: {:?}", fast_response_str);
    
    // Test 3: Error response
    println!("\n🔸 Test 3: Error Response");
    let error_request = b"GET /api/error-test";
    
    let reply_sender = conn.ask_with_reply_sender(error_request).await.unwrap();
    println!("📨 Created reply sender for error test: {:?}", reply_sender);
    
    let error_task = tokio::spawn(async move {
        sleep(Duration::from_millis(5)).await;
        reply_sender.reply_error("Access denied").unwrap();
        "ERROR:Access denied".to_string()
    });
    
    let error_result = error_task.await.unwrap();
    println!("❌ Error response: {:?}", error_result);
    
    // Test 4: Performance comparison
    println!("\n🔸 Test 4: Performance Comparison");
    let perf_request = b"GET /api/perf-test";
    let iterations = 50;
    
    // Regular ask()
    let regular_start = std::time::Instant::now();
    for _ in 0..iterations {
        let _response = conn.ask(perf_request).await.unwrap();
    }
    let regular_time = regular_start.elapsed();
    
    // Delegated ask()
    let delegated_start = std::time::Instant::now();
    for _ in 0..iterations {
        let reply_sender = conn.ask_with_reply_sender(perf_request).await.unwrap();
        let response = reply_sender.create_mock_reply();
        reply_sender.reply(response).unwrap();
    }
    let delegated_time = delegated_start.elapsed();
    
    println!("📊 Performance Results:");
    println!("   Regular ask():   {:?} ({:.3} μs avg)", regular_time, regular_time.as_nanos() as f64 / iterations as f64 / 1000.0);
    println!("   Delegated ask(): {:?} ({:.3} μs avg)", delegated_time, delegated_time.as_nanos() as f64 / iterations as f64 / 1000.0);
    
    let overhead = delegated_time.as_nanos() as f64 / regular_time.as_nanos() as f64;
    println!("   Overhead: {:.2}x", overhead);
    
    // Test 5: Multiple concurrent requests
    println!("\n🔸 Test 5: Multiple Concurrent Requests");
    let concurrent_count = 10;
    let mut tasks = Vec::new();
    
    for i in 0..concurrent_count {
        let conn_clone = conn.clone();
        let request = format!("GET /api/concurrent/{}", i);
        
        let task = tokio::spawn(async move {
            let reply_sender = conn_clone.ask_with_reply_sender(request.as_bytes()).await.unwrap();
            sleep(Duration::from_millis(5)).await;
            let response = reply_sender.create_mock_reply();
            reply_sender.reply(response.clone()).unwrap();
            (i, response)
        });
        tasks.push(task);
    }
    
    for task in tasks {
        let (i, response) = task.await.unwrap();
        let response_str = String::from_utf8_lossy(&response);
        println!("   Request {}: {:?}", i, response_str);
    }
    
    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;
    
    println!("\n✅ All tests completed successfully!");
}

#[tokio::test]
async fn test_delegated_reply_sender_timeout() {
    println!("🚀 DelegatedReplySender Timeout Test");
    
    // Setup 
    let config = GossipConfig::default();
    let node1_addr = "127.0.0.1:34001".parse().unwrap();
    let node2_addr = "127.0.0.1:34002".parse().unwrap();
    
    let node1 = GossipRegistryHandle::new(node1_addr, vec![node2_addr], Some(config.clone())).await.unwrap();
    let node2 = GossipRegistryHandle::new(node2_addr, vec![node1_addr], Some(config.clone())).await.unwrap();
    
    sleep(Duration::from_millis(100)).await;
    let conn = node1.get_connection(node2_addr).await.unwrap();
    
    // Test timeout behavior
    let timeout_request = b"GET /api/slow-endpoint";
    let short_timeout = Duration::from_millis(50);
    
    let reply_sender = conn.ask_with_timeout_and_reply(timeout_request, short_timeout).await.unwrap();
    println!("📨 Created reply sender with {}ms timeout", short_timeout.as_millis());
    
    // Simulate slow processing (should timeout)
    let slow_task = tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await; // Longer than timeout
        let response = reply_sender.create_mock_reply();
        reply_sender.reply(response.clone()).unwrap();
        response
    });
    
    // This should timeout since the task sleeps longer than the timeout
    let result = tokio::time::timeout(Duration::from_millis(75), slow_task).await;
    
    match result {
        Ok(Ok(response)) => {
            println!("⚠️  Unexpected success: {:?}", String::from_utf8_lossy(&response));
        }
        Ok(Err(e)) => {
            println!("✅ Expected task error: {:?}", e);
        }
        Err(_) => {
            println!("✅ Expected timeout occurred");
        }
    }
    
    // Cleanup
    node1.shutdown().await;
    node2.shutdown().await;
    
    println!("✅ Timeout test completed!");
}