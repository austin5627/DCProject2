use std::collections::HashMap;
use std::env::args;

#[derive(Debug)]
struct Node {
    ip: String,
    port: i32,
    edges: Vec<(i32, i32)>, // (node_id, weight)
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

fn main() {
    let config_file = args().nth(1).expect("Usage: cargo run <config_file> <node_id>");
    let node_id = args()
        .nth(2)
        .expect("Usage: cargo run <config_file> <node_id>")
        .parse::<i32>()
        .expect("Node ID must be an integer");
    let nodes = read_config(&config_file);

    if let Some(node) = nodes.get(&node_id) {
        println!("Node {} found", node_id);
        println!("\tIP: {}", node.ip);
        println!("\tPort: {}", node.port);
        println!("\tEdges: ");
        for edge in node.edges.iter() {
            println!("\t\t{} -> {} ({})", node_id, edge.0, edge.1);
        }
    } else {
        println!("Node {} not found", node_id);
    }

}
