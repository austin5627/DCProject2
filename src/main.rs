use std::collections::{HashMap, HashSet};
use std::env::args;
use std::fmt::{Debug, Display};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};

use std::thread::sleep;
use std::time::Duration;

use owo_colors::OwoColorize;
use serde::{Deserialize, Serialize};

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
    Response(i32, i32),
    // (component id, other node)
    Join,
}

impl Display for MessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageType::Sync => write!(f, "{}", "Sync".red()),
            MessageType::Stop => write!(f, "{}", "Stop".red()),
            MessageType::Done => write!(f, "{}", "Done".red()),
            MessageType::BroadcastNewLeader(id) => {
                write!(f, "{}({})", "BroadcastNewLeader".blue(), id)
            }
            MessageType::ConvergeMinWeight(id, weight) => {
                write!(f, "{}({}, {})", "ConvergeMinWeight".purple(), id, weight)
            }
            MessageType::BroadcastMinWeight(id, weight) => {
                write!(f, "{}({}, {})", "BroadcastMinWeight".blue(), id, weight)
            }
            MessageType::Search => write!(f, "{}", "Search".yellow()),
            MessageType::Response(comp_id, other_id) => {
                write!(f, "{}({} {})", "Response".green(), comp_id, other_id)
            }
            MessageType::Join => write!(f, "{}", "Join".cyan()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    msg_type: MessageType,
    sender: i32,
    receiver: i32,
}

fn send_message(msg: Message, stream: &mut TcpStream) {
    let bytes: Vec<u8> = msg.into();
    let len = bytes.len() as u32;
    let len_bytes = len.to_le_bytes();
    stream.write(&len_bytes).expect("Unable to write to stream");
    stream.write(&bytes).expect("Unable to write to stream");
}

fn recv_message(stream: &mut TcpStream) -> Option<Message> {
    let mut len_bytes = [0; 4];
    stream.read_exact(&mut len_bytes).ok()?;
    let len = u32::from_le_bytes(len_bytes);
    let mut bytes = vec![0; len as usize];
    stream.read_exact(&mut bytes).ok()?;
    let msg: Message = bytes[..].into();
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
        let mut socket: TcpStream;
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
        send_message(
            Message {
                msg_type: MessageType::Sync,
                sender: node_id,
                receiver: *neighbor,
            },
            &mut socket.try_clone().expect("Unable to clone socket"),
        );
        let msg = recv_message(&mut socket).expect("Unable to receive message");
        let connected_node_id = msg.sender;
        println!("Connection established with {} {}", addr, connected_node_id);

        listeners.insert(connected_node_id, socket);
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
    let mut global_min_weight_out_edge: (i32, i32) = (-1, i32::MAX); // (node_id, weight)
    let mut weights_received: HashSet<i32> = HashSet::new();
    let mut start_next_phase = false;
    let mut phase = 0;
    let mut done = HashSet::new();
    let mut stop = HashSet::new();

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
        println!("Sending {} to {}", msg.msg_type, neighbor);
        send_message(
            msg,
            streams.get_mut(neighbor).expect("Unable to get stream"),
        );
    }
    let mut round = 0;
    loop {
        round += 1;
        // Send search message to neighbors
        // Receive response from neighbors
        // Store min weight edge from neighbor in other component
        // once all external neighbors and tree children have responded , converge cast min weight edge
        // if leader find min weight edge and broadcast to all tree neighbors
        // if not leader, receive min weight edge from leader and broadcast to all tree neighbors
        // if node with min weight edge, send join message to external neighbor with min weight edge
        // if both sender and receiver of join message you are new leader, send broadcast new leader message to all tree neighbors
        // if receive broadcast new leader message, update leader and parent, continue broadcast to all tree neighbors

        if done.len() == node.edges.len() && stop.len() == node.edges.len() {
            println!("All neighbors done, breaking");
            for (id, stream) in &mut streams {
                send_message(
                    Message {
                        msg_type: MessageType::Stop,
                        sender: node_id,
                        receiver: *id,
                    },
                    stream,
                );
            }
            break;
        }

        let mut to_send: HashMap<i32, Message> = HashMap::new();
        for (id, stream) in &mut streams {
            if let Some(msg) = recv_message(stream) {
                if msg.msg_type != MessageType::Sync {
                    println!(
                        "Received message: {} from {} round {}",
                        msg.msg_type, msg.sender, round
                    );
                    println!("\tCurrent component: {}", leader);
                    println!("\tCurrent span edges: {:?}", span_edges);
                } else {
                    print!("\r");
                }
                match msg.msg_type {
                    MessageType::Response(component_id, _) => {
                        if component_id != leader {
                            min_weight_out_edge = node.edges[(last_edge_checked) as usize];
                            println!("\tFound min weight edge: {:?}", min_weight_out_edge);
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
                                        msg_type: MessageType::ConvergeMinWeight(
                                            node_id,
                                            min_weight_out_edge.1,
                                        ),
                                        sender: node_id,
                                        receiver: parent,
                                    },
                                );
                            }
                        } else if last_edge_checked < node.edges.len() as i32 - 1 {
                            last_edge_checked += 1;
                            let recipient = node.edges[last_edge_checked as usize].0;
                            to_send.insert(
                                recipient,
                                Message {
                                    msg_type: MessageType::Search,
                                    sender: node_id,
                                    receiver: recipient,
                                },
                            );
                        }
                    }
                    MessageType::Search => {
                        to_send.insert(
                            *id,
                            Message {
                                msg_type: MessageType::Response(leader, *id),
                                sender: node_id,
                                receiver: *id,
                            },
                        );
                    }
                    MessageType::BroadcastNewLeader(leader_id) => {
                        leader = leader_id;
                        parent = *id;
                        start_next_phase = true;
                        println!("\tNew leader: {} my parent: {}", leader, parent);
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
                    MessageType::ConvergeMinWeight(min_id, weight) => {
                        if weight < global_min_weight_out_edge.1 {
                            global_min_weight_out_edge = (min_id, weight);
                        }
                        weights_received.insert(min_id);
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
                            println!(
                                "\tConverged on min weight edge: {:?}",
                                global_min_weight_out_edge
                            );
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
                        } else {
                            println!(
                                "\tWaiting for more weights {weights_received:?} {span_edges:?}"
                            );
                        }
                    }
                    MessageType::BroadcastMinWeight(min_id, weight) => {
                        if min_id == node_id {
                            min_weight_out_edge = (min_id, weight);
                            println!("\tI have the min weight edge: {:?} ", min_weight_out_edge);
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
                    },

                    // 5 -> join -> 7
                    // 7 -> join -> 5
                    MessageType::Join => {
                        span_edges.insert(msg.sender);
                        println!(
                            "\tJoining node: {} MWOE: {:?}",
                            msg.sender, min_weight_out_edge
                        );
                        if msg.sender == min_weight_out_edge.0 && msg.sender < node_id {
                            println!("\tI am the new leader!");
                            start_next_phase = true;
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
                        } else if start_next_phase {
                            to_send.insert(
                                parent,
                                Message {
                                    msg_type: MessageType::BroadcastNewLeader(leader),
                                    sender: node_id,
                                    receiver: parent,
                                },
                            );
                        }
                    }
                    MessageType::Done => {
                        println!("{} is done", id);
                        done.insert(*id);
                    }
                    MessageType::Stop => {
                        stream
                            .shutdown(Shutdown::Both)
                            .expect("shutdown call failed");
                    }
                    MessageType::Sync => {}
                }
            }
        }
        for (id, stream) in &mut streams {
            if last_edge_checked == (node.edges.len()) as i32 && !stop.contains(id) {
                send_message(
                    Message {
                        msg_type: MessageType::Done,
                        sender: node_id,
                        receiver: *id,
                    },
                    stream,
                );
                stop.insert(*id);
                // continue;
            } else if span_edges.contains(&node.edges[last_edge_checked as usize].0) {
                last_edge_checked += 1;
                continue;
            }

            if start_next_phase
                && to_send.is_empty()
                && last_edge_checked < (node.edges.len() - 1) as i32
                && *id == node.edges[(last_edge_checked + 1) as usize].0
                && !stop.contains(id)
            {
                phase += 1;
                start_next_phase = false;
                println!("Starting phase {}", phase);
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
            if !to_send.contains_key(id) {
                send_message(
                    Message {
                        msg_type: MessageType::Sync,
                        sender: node_id,
                        receiver: *id,
                    },
                    stream,
                );
                println!("Sending {} to {}", MessageType::Sync, id);
            } else {
                let msg = to_send.remove(id).expect("Unable to get message");
                println!("Sending message: {} to {}", msg.msg_type, id);
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
