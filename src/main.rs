extern crate bincode;
extern crate serde;

use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::env::args;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};

use std::thread::sleep;
use std::time::Duration;

use serde::{Deserialize, Serialize};

const MAX_MESSAGE_SIZE: usize = 4096;

#[derive(Debug)]
struct Node {
    ip: String,
    port: i32,
    edges: Vec<(i32, i32)>, // (node_id, weight)
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
enum MessageType {
    Sync,
    Stop,
    Done,
    BroadcastNewLeader(i32),
    // (leader id)
    ConvergeMinWeight(i32, i32),
    // (min id, min weight)
    BroadcastMinWeight(i32, i32),
    // (min id, min weight)
    Search,
    // take me to your leader!
    Response(i32),
    // (component id)
    Join,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    msg_type: MessageType,
    sender: i32,
    receiver: i32,
}

fn send_message(msg: Message, stream: &mut TcpStream) {
    let bytes: Vec<u8> = msg.into();
    stream.write(&bytes).expect("Unable to write to stream");
}

fn recv_message(stream: &mut TcpStream) -> Option<Message> {
    let mut buf = vec![0; MAX_MESSAGE_SIZE];
    let n = stream.read(&mut buf).expect("Unable to read from stream");
    if n == 0 {
        return None;
    }
    let msg: Message = buf[..n].into();
    Some(msg)
}

impl From<&[u8]> for Message {
    fn from(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("Unable to deserialize message")
    }
}

impl From<Message> for Vec<u8> {
    fn from(msg: Message) -> Self {
        bincode::serialize(&msg).expect("Unable to serialize message")
    }
}

fn read_config(filename: &str) -> HashMap<i32, Node> {
    let file_string = std::fs::read_to_string(filename).expect("Failed to read config file");
    let mut nodes: HashMap<i32, Node> = HashMap::new();

    let mut lines = file_string
        .lines()
        .map(|line| line.trim())
        .filter(|line| line.starts_with(|c: char| c.is_digit(10) || c == '('));

    let num_nodes = lines
        .next()
        .expect("Config file is empty")
        .parse::<i32>()
        .expect("First line of config file must be an integer");

    for _ in 0..num_nodes {
        if let Some(line) = lines.next() {
            let mut parts = line.split_whitespace();
            let id = parts
                .next()
                .expect("Node ID not found")
                .parse::<i32>()
                .expect("Node ID must be an integer");
            let ip = parts.next().expect("IP not found").to_string();
            let port = parts
                .next()
                .expect("Port not found")
                .parse::<i32>()
                .expect("Port must be an integer");

            let edges: Vec<(i32, i32)> = Vec::new();
            let node = Node { ip, port, edges };
            nodes.insert(id, node);
        }
    }

    while let Some(line) = lines.next() {
        let mut parts = line.split_whitespace();
        let edge = parts
            .next()
            .expect("Edge line is empty")
            .trim_matches(|c| c == '(' || c == ')')
            .split(',')
            .map(|s| s.parse::<i32>().expect("Node id be an integer"))
            .collect::<Vec<i32>>();
        let weight = parts
            .next()
            .expect("Weight not found")
            .parse::<i32>()
            .expect("Weight must be an integer");
        nodes
            .get_mut(&edge[0])
            .expect("Node {edge[0]} not found")
            .edges
            .append(&mut vec![(edge[1], weight)]);
        nodes
            .get_mut(&edge[1])
            .expect("Node {edge[1]} not found")
            .edges
            .append(&mut vec![(edge[0], weight)]);
    }

    for node in nodes.values_mut() {
        node.edges.sort_by(|a, b| a.1.cmp(&b.1));
    }

    return nodes;
}

fn connect_to_neighbors(node_id: i32, nodes: &HashMap<i32, Node>) -> HashMap<i32, TcpStream> {
    let node = nodes.get(&node_id).expect("Unable to find node id");
    let listener =
        TcpListener::bind(format!("{}:{}", node.ip, node.port)).expect("Unable to bind to port");
    let mut listeners = HashMap::new();
    for (neighbor, _) in &node.edges {
        let socket: TcpStream;
        let addr: SocketAddr;
        if node_id < *neighbor {
            loop {
                match TcpStream::connect(format!("{}:{}", nodes[neighbor].ip, nodes[neighbor].port))
                {
                    Ok(s) => {
                        socket = s;
                        addr = socket.peer_addr().expect("Unable to get peer address");
                        break;
                    }
                    Err(_) => {
                        println!("Unable to connect to {}, retrying...", neighbor);
                        sleep(Duration::from_secs(1));
                    }
                }
            }
        } else {
            // accept connections from neighbors with lower id
            let conn = listener.accept().expect("Unable to accept connection");
            socket = conn.0;
            addr = conn.1;
        }
        println!("Connection established with {}", addr);

        listeners.insert(*neighbor, socket);
    }
    return listeners;
}

fn sync_ghs(nodes: &HashMap<i32, Node>, node_id: i32) {
    let node = nodes.get(&node_id).expect("Unable to find node id");
    let mut streams = connect_to_neighbors(node_id, &nodes);
    // level 0 is each node by itself
    println!("Node {} is connected to {:?}", node_id, streams.keys());

    let mut leader = node_id;
    let mut span_edges: HashSet<i32> = HashSet::new();
    let mut parent: i32 = node_id;
    let mut last_edge_checked = 0;

    let mut min_weight_out_edge = node.edges[0];
    let mut global_min_weight_out_edge: (i32, i32) = (-1, -1); // (node_id, weight)
    let mut weights_received: HashSet<i32> = HashSet::new();
    let mut start_next_level = false;

    // Send search message to first neighbor and sync to the rest
    let mut i = 0;
    for (neighbor, _) in &node.edges {
        let msg_type;
        if i == 0 {
            msg_type = MessageType::Search;
            i += 1;
        } else {
            msg_type = MessageType::Sync;
        }
        let msg = Message {
            msg_type,
            sender: node_id,
            receiver: *neighbor,
        };
        println!("Sending {:?} message to {}", msg.msg_type, neighbor);
        send_message(
            msg,
            streams.get_mut(neighbor).expect("Unable to get stream"),
        );
    }
    loop {
        // Send search message to neighbors
        // Receive response from neighbors
        // Store min weight edge from neighbor in other component
        // once all external neighbors and tree children have responded , converge cast min weight edge
        // if leader find min weight edge and broadcast to all tree neighbors
        // if not leader, receive min weight edge from leader and broadcast to all tree neighbors
        // if node with min weight edge, send join message to external neighbor with min weight edge
        // if both sender and receiver of join message you are new leader, send broadcast new leader message to all tree neighbors
        // if receive broadcast new leader message, update leader and parent, continue broadcast to all tree neighbors
        if last_edge_checked == node.edges.len() as i32 {
            println!("All edges checked, breaking");
            break;
        }

        let mut to_send: HashMap<i32, Message> = HashMap::new();
        for (id, stream) in &mut streams {
            if let Some(msg) = recv_message(stream) {
                if msg.msg_type != MessageType::Sync {
                    println!("Received message: {:?}", msg);
                    println!("Current component: {}", leader);
                    println!("Current span edges: {:?}", span_edges);
                }
                match msg.msg_type {
                    MessageType::Response(component_id) => {
                        if component_id != leader {
                            min_weight_out_edge = node.edges[(last_edge_checked + 1) as usize];
                            println!("Found min weight edge: {:?}", min_weight_out_edge);
                            if span_edges.len() == 0 {
                                to_send.insert(
                                    *id,
                                    Message {
                                        msg_type: MessageType::Join,
                                        sender: node_id,
                                        receiver: *id,
                                    },
                                );
                                span_edges.insert(*id);
                            } else {
                                to_send.insert(
                                    parent,
                                    Message {
                                        msg_type: MessageType::ConvergeMinWeight(min_weight_out_edge.0, min_weight_out_edge.1),
                                        sender: node_id,
                                        receiver: parent,
                                    },
                                );
                            }
                        } else {
                            last_edge_checked += 1;
                            to_send.insert(
                                *id,
                                Message {
                                    msg_type: MessageType::Search,
                                    sender: node_id,
                                    receiver: *id,
                                },
                            );
                        }
                    }
                    MessageType::Search => {
                        to_send.insert(
                            *id,
                            Message {
                                msg_type: MessageType::Response(leader),
                                sender: node_id,
                                receiver: *id,
                            },
                        );
                    }
                    MessageType::BroadcastNewLeader(leader_id) => {
                        leader = leader_id;
                        parent = *id;
                        start_next_level = true;
                        for n in &span_edges {
                            if *n != parent {
                                to_send.insert(
                                    *n,
                                    Message {
                                        msg_type: MessageType::BroadcastNewLeader(leader_id),
                                        sender: node_id,
                                        receiver: *n,
                                    },
                                );
                            }
                        }
                    }
                    MessageType::ConvergeMinWeight(node_id, weight) => {
                        if weight < global_min_weight_out_edge.1 {
                            global_min_weight_out_edge = (node_id, weight);
                        }
                        weights_received.insert(node_id);
                        if weights_received.len() == span_edges.len() - 1 && node_id != leader {
                            to_send.insert(
                                parent,
                                Message {
                                    msg_type: MessageType::ConvergeMinWeight(
                                        global_min_weight_out_edge.0,
                                        global_min_weight_out_edge.1,
                                    ),
                                    sender: node_id,
                                    receiver: leader,
                                },
                            );
                        } else if weights_received.len() == span_edges.len() && node_id == leader {
                            for n in &span_edges {
                                if *n != parent {
                                    to_send.insert(
                                        *n,
                                        Message {
                                            msg_type: MessageType::BroadcastMinWeight(
                                                global_min_weight_out_edge.0,
                                                global_min_weight_out_edge.1,
                                            ),
                                            sender: node_id,
                                            receiver: *n,
                                        },
                                    );
                                }
                            }
                        }
                    }
                    MessageType::BroadcastMinWeight(min_id, weight) => {
                        if min_id == node_id {
                            to_send.insert(
                                min_weight_out_edge.0,
                                Message {
                                    msg_type: MessageType::Join,
                                    sender: node_id,
                                    receiver: min_weight_out_edge.0,
                                },
                            );
                            span_edges.insert(min_weight_out_edge.0);
                        } else {
                            for n in &span_edges {
                                if *n != parent {
                                    to_send.insert(
                                        *n,
                                        Message {
                                            msg_type: MessageType::BroadcastMinWeight(
                                                min_id, weight,
                                            ),
                                            sender: node_id,
                                            receiver: *n,
                                        },
                                    );
                                }
                            }
                        }
                    }

                    // 5 -> join -> 7
                    // 7 -> join -> 5
                    MessageType::Join => {
                        span_edges.insert(msg.sender);
                        if msg.sender == min_weight_out_edge.0 && msg.sender < node_id {
                            leader = node_id;
                            parent = node_id;
                            for n in &span_edges {
                                if *n != parent {
                                    to_send.insert(
                                        *n,
                                        Message {
                                            msg_type: MessageType::BroadcastNewLeader(node_id),
                                            sender: node_id,
                                            receiver: *n,
                                        },
                                    );
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        for (id, stream) in &mut streams {
            if start_next_level && to_send.is_empty() && *id == node.edges[(last_edge_checked + 1) as usize].0 {
                to_send.insert(
                    *id,
                    Message {
                        msg_type: MessageType::Search,
                        sender: node_id,
                        receiver: *id,
                    },
                );
                continue;
            }
            if !to_send.contains_key(id) {
                send_message(
                    Message {
                        msg_type: MessageType::Sync,
                        sender: node_id,
                        receiver: *id,
                    },
                    stream,
                );
            } else {
                let msg = to_send.remove(id).expect("Unable to get message");
                println!("Sending message: {:?}", msg);
                send_message(msg, stream);
            }
        }
    }
}

fn main() {
    let config_file = args()
        .nth(1)
        .expect("Usage: cargo run <config_file> <node_id>");
    let node_id = args()
        .nth(2)
        .expect("Usage: cargo run <config_file> <node_id>")
        .parse::<i32>()
        .expect("Node ID must be an integer");
    let nodes = read_config(&config_file);
    sync_ghs(&nodes, node_id);
}
