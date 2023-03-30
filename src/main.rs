use std::collections::{HashMap, HashSet};
use std::env::args;
use std::fmt::{Debug, Display};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

use std::thread::sleep;
use std::time::Duration;

use owo_colors::OwoColorize;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct Node {
    ip: String,
    port: i32,
    edges: Vec<(i32, (i32, i32, i32))>, // neighbor id (weight, smaller id, larger id)
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq)]
enum Message {
    Request,
    Search,
    // take me to your leader!
    Response(i32),
    // (component id)
    ConvergeMinWeight(i32, i32, i32),
    // (min id, min weight)
    BroadcastMinWeight(i32, i32, i32),
    // (min id, min weight)
    BroadcastNewLeader(i32),
    // (leader id)
    ConfirmLeader,
    Join,
    Sync,
    Done,
}

impl Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::Sync => write!(f, "{}", "Sync".red()),
            Message::Done => write!(f, "{}", "Done".red()),
            Message::BroadcastNewLeader(id) => {
                write!(f, "{}({})", "BroadcastNewLeader".blue(), id)
            }
            Message::ConfirmLeader => write!(f, "{}", "ConfirmLeader".blue()),
            Message::ConvergeMinWeight(id, dest, weight) => {
                write!(
                    f,
                    "{}({} {} {})",
                    "ConvergeMinWeight".purple(),
                    id,
                    dest,
                    weight
                )
            }
            Message::BroadcastMinWeight(id, dest, weight) => {
                write!(
                    f,
                    "{}({} {} {})",
                    "BroadcastMinWeight".blue(),
                    id,
                    dest,
                    weight
                )
            }
            Message::Search => write!(f, "{}", "Search".yellow()),
            Message::Request => write!(f, "{}", "Request".yellow()),
            Message::Response(comp_id) => {
                write!(f, "{}({})", "Response".green(), comp_id)
            }
            Message::Join => write!(f, "{}", "Join".cyan()),
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

fn send_message(msg: Message, stream: &mut TcpStream) -> Result<(), std::io::Error> {
    let bytes: Vec<u8> = msg.into();
    let len = bytes.len() as u32;
    let len_bytes = len.to_le_bytes();
    stream.write(&len_bytes)?;
    stream.write(&bytes)?;
    Ok(())
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

            let edges = Vec::new();
            let node = Node { ip, port, edges };
            nodes.insert(id, node);
        }
    }

    while let Some(line) = lines.next() {
        let mut parts = line.split_whitespace();
        // smaller id, larger id
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
            .append(&mut vec![(edge[1], (weight, edge[0], edge[1]))]);
        nodes
            .get_mut(&edge[1])
            .expect("Node {edge[1]} not found")
            .edges
            .append(&mut vec![(edge[0], (weight, edge[0], edge[1]))]);
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
            Message::Response(node_id),
            &mut socket.try_clone().expect("Unable to clone socket"),
        )
        .expect("Unable to send message");
        let msg = recv_message(&mut socket).expect("Unable to receive message");
        if let Message::Response(other_id) = msg {
            println!("Connection established with {} {}", addr, other_id);
            listeners.insert(other_id, socket);
        } else {
            panic!("Unexpected message type");
        }
    }
    return listeners;
}

fn sync_ghs(nodes: &HashMap<i32, Node>, node_id: i32) {
    let node = nodes.get(&node_id).expect("Unable to find node id");
    let mut streams = connect_to_neighbors(node_id, &nodes);
    // level 0 is each node by itself
    println!("Node {} is connected to {:?}", node_id, node.edges);

    let mut leader = node_id;

    let mut span_edges: HashSet<i32> = HashSet::new();
    let mut join_recv: HashSet<i32> = HashSet::new();
    let mut join_sent: Option<i32> = None;
    let mut unchecked_edges: HashSet<i32> = node.edges.iter().map(|e| e.0).collect();

    let mut parent: i32 = node_id;
    let mut next_edge = 0;

    let mut min_weight_out_edge: (i32, i32, i32) = (-1, -1, i32::MAX); // (node_id, weight)

    let mut weights_received: HashSet<i32> = HashSet::new();
    let mut confirm: HashSet<i32> = HashSet::new();

    let mut start_next_phase = false;
    let mut phase = 0;
    let mut round = 0;

    let mut broadcast: Option<Message> = None;
    let mut to_send: HashMap<i32, Vec<Message>> = HashMap::new();

    let mut done = HashSet::new();

    // Send search message to first neighbor and sync to the rest
    for i in 0..node.edges.len() {
        let msg = if i == 0 {
            Message::Search
        } else {
            Message::Sync
        };
        println!("Sending {} to {}", msg, node.edges[i].0);
        send_message(
            msg,
            streams
                .get_mut(&node.edges[i].0)
                .expect("Unable to find stream"),
        )
        .expect("Unable to send message");
    }
    'outer: loop {
        for (id, stream) in &mut streams {
            if let Some(msg) = recv_message(stream) {
                if msg != Message::Sync {
                    println!("{} Received {} from {}", node_id, msg, id);
                }
                match msg {
                    Message::Request => {
                        start_next_phase = true;
                    }
                    Message::Search => {
                        // Send response with component id
                        // to_send.insert(*id, Message::Response(leader));
                        to_send
                            .entry(*id)
                            .or_insert(vec![])
                            .push(Message::Response(leader));
                    }
                    Message::Response(component_id) => {
                        // if component_id != node_id, they are mwoe
                        // if component_id == node_id, send search to next neighbor
                        if component_id != leader {
                            weights_received.insert(node_id);
                            let weight = node.edges[next_edge].1 .0;
                            if weight < min_weight_out_edge.2 {
                                min_weight_out_edge = (node_id, *id, weight);
                            }
                        } else {
                            next_edge += 1;
                            unchecked_edges.remove(id);
                            if next_edge < node.edges.len() - 1 {
                                while !unchecked_edges.contains(&node.edges[next_edge].0) {
                                    next_edge += 1;
                                }
                                to_send
                                    .entry(node.edges[next_edge].0)
                                    .or_insert(vec![])
                                    .push(Message::Search);
                            } else {
                                weights_received.insert(node_id);
                            }
                        }
                    }
                    Message::ConvergeMinWeight(min_id, dest, weight) => {
                        // if leader broadcast min weight
                        // if not leader, aggregate min weight from children and your own
                        // send min weight to parent
                        weights_received.insert(*id);
                        if weight < min_weight_out_edge.2 {
                            min_weight_out_edge = (min_id, dest, weight);
                        }
                    }
                    Message::BroadcastMinWeight(min_id, dest, weight) => {
                        // broadcast min weight to children
                        // if min_id == node_id, send join to min weight neighbor
                        min_weight_out_edge = (min_id, dest, weight);
                        if min_id == node_id {
                            to_send.entry(dest).or_insert(vec![]).push(Message::Join);
                            unchecked_edges.remove(&dest);
                            next_edge += 1;
                        }
                        broadcast = Some(msg);
                    }
                    Message::BroadcastNewLeader(leader_id) => {
                        // broadcast new leader to children
                        // update leader
                        // start next phase
                        leader = leader_id;
                        parent = *id;
                        broadcast = Some(msg);
                        if !join_recv.is_empty() {
                            span_edges.extend(&join_recv);
                            join_recv.clear();
                        }
                        if let Some(j_id) = join_sent {
                            span_edges.insert(j_id);
                            join_sent = None;
                        }
                        min_weight_out_edge = (-1, -1, i32::MAX);
                        if span_edges.len() == 1 {
                            to_send
                                .entry(parent)
                                .or_insert(vec![])
                                .push(Message::ConfirmLeader);
                        }
                    }
                    Message::ConfirmLeader => {
                        confirm.insert(*id);
                        if confirm.len() == span_edges.len() && leader == node_id {
                            broadcast = Some(Message::Request);
                            start_next_phase = true;
                            confirm.clear();
                        } else if confirm.len() + 1 == span_edges.len() && leader != node_id {
                            to_send
                                .entry(parent)
                                .or_insert(vec![])
                                .push(Message::ConfirmLeader);
                            confirm.clear();
                        }
                    }
                    Message::Join => {
                        // if receiving from min weight neighbor, and their id < node_id you are
                        // the new leader
                        // broadcast new leader to all tree neighbors then start next phase
                        join_recv.insert(*id);
                        if unchecked_edges.contains(id) {
                            unchecked_edges.remove(id);
                        }
                    }
                    Message::Sync => {}
                    Message::Done => {
                        done.insert(*id);
                        if leader == node_id
                            && span_edges.difference(&done).count() == 0
                            && done.len() == node.edges.len() + 1
                        {
                            break 'outer;
                        }
                    }
                }
            }
        }

        if next_edge == node.edges.len()
            && to_send.is_empty()
            && broadcast.is_none()
            && !done.contains(&node_id)
        {
            done.insert(node_id);
            for (id, _) in &node.edges {
                to_send.entry(*id).or_insert(vec![]).push(Message::Done);
            }
            println!("{} All edges checked", node_id);
        }
        if weights_received.union(&done).count() >= span_edges.len()
            && min_weight_out_edge != (-1, -1, i32::MAX)
            && to_send.is_empty()
        {
            if parent != node_id {
                to_send
                    .entry(parent)
                    .or_insert(vec![])
                    .push(Message::ConvergeMinWeight(
                        min_weight_out_edge.0,
                        min_weight_out_edge.1,
                        min_weight_out_edge.2,
                    ));
                min_weight_out_edge = (-1, -1, i32::MAX);
                weights_received.clear();
            } else if weights_received.len() == span_edges.len() + 1 {
                if min_weight_out_edge.0 == node_id {
                    to_send
                        .entry(min_weight_out_edge.1)
                        .or_insert(vec![])
                        .push(Message::Join);
                    join_sent = Some(min_weight_out_edge.1);
                    next_edge += 1;
                }
                broadcast = Some(Message::BroadcastMinWeight(
                    min_weight_out_edge.0,
                    min_weight_out_edge.1,
                    min_weight_out_edge.2,
                ));
                // min_weight_out_edge = (-1, -1, i32::MAX);
                weights_received.clear();
            }
        }

        if start_next_phase && next_edge < node.edges.len() && to_send.is_empty() {
            broadcast = Some(Message::Request);
            for i in next_edge..node.edges.len() {
                next_edge = i;
                if unchecked_edges.contains(&node.edges[i].0) {
                    to_send
                        .entry(node.edges[i].0)
                        .or_insert(vec![])
                        .push(Message::Search);
                    break;
                } else {
                    next_edge += 1;
                }
            }
            if next_edge == node.edges.len() {
                weights_received.insert(node_id);
            }
            start_next_phase = false;
            min_weight_out_edge = (-1, -1, i32::MAX);
            phase += 1;
        }

        if let Some(j_id) = join_sent {
            if join_recv.contains(&j_id) && node_id > j_id {
                broadcast = Some(Message::BroadcastNewLeader(node_id));
                start_next_phase = true;
                span_edges.extend(&join_recv);
                join_sent = None;
                join_recv.clear();
            }
        }

        if let Some(msg) = broadcast {
            for id in &span_edges {
                if parent != *id {
                    to_send.entry(*id).or_insert(vec![]).push(msg);
                }
            }
            broadcast = None;
        }

        for (id, stream) in &mut streams {
            let msg: Message = if let Some(msgs) = to_send.get(id) {
                if let Some(m) = msgs.first() {
                    println!("{} Sending {} to {}", node_id, m, id);
                    if m == &Message::Join {
                        join_sent = Some(*id);
                    }
                    *m
                } else {
                    Message::Sync
                }
            } else {
                Message::Sync
            };

            if let Err(_) = send_message(msg, stream) {
                break 'outer;
            }
        }
        to_send.clear();
    }
    println!("{} Quitting", node_id);
    println!("\n\n\n");
    println!("Leader: {}", leader);
    if parent != node_id {
        println!("Parent: {}", parent);
    }
    println!("Minimum Spanning Tree edges: {:?}", span_edges);
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
