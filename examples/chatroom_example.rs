//! Chatroom Example
//!
//! Demonstrates the decentralized chatroom protocol built on Chaincraft.
//! Ported from the Python chaincraft examples (chatroom_protocol.py).
//!
//! Features:
//! - Create chatrooms with admin control
//! - Post messages to chatrooms
//! - ECDSA-signed messages for authentication
//!
//! Run with: `cargo run --example chatroom_example`

use chaincraft::{
    crypto::ecdsa::ECDSASigner,
    error::Result,
    examples::chatroom::{helpers, ChatroomNode},
};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    println!("Chaincraft Chatroom Example");
    println!("===========================\n");

    let mut node = ChatroomNode::new(0).await?;
    node.start().await?;

    println!("Node started with peer ID: {}", node.id());

    let admin_signer = ECDSASigner::new()?;

    let create_msg = helpers::create_chatroom_message("rust_chatroom".to_string(), &admin_signer)?;
    node.publish(create_msg).await?;

    println!("Created chatroom 'rust_chatroom'");
    sleep(Duration::from_millis(200)).await;

    let post_msg = helpers::create_post_message(
        "rust_chatroom".to_string(),
        "Hello from Chaincraft Rust!".to_string(),
        &admin_signer,
    )?;
    node.publish(post_msg).await?;

    println!("Posted message to chatroom");
    sleep(Duration::from_millis(200)).await;

    let names = node.chatroom_names().await?;
    println!("\nChatrooms: {names:?}");
    println!(
        "Messages in rust_chatroom: {}",
        node.chatroom_message_count("rust_chatroom").await?
    );

    println!("\nShutting down...");
    node.close().await?;
    println!("Done.");

    Ok(())
}
