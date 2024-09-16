use std::error::Error;
use std::io;
use std::time::Duration;

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event as CEvent, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use dotenvy::dotenv;
use redis::{AsyncCommands, FromRedisValue, RedisError};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time;
use tui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Span, Spans},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs},
    Frame, Terminal,
};

// Define Node and Room structures
#[derive(Debug, Deserialize, Clone)]
struct Node {
    address: String,
    num_rooms: u32,
    num_connections: u32,
    cpu_usage: f64,
    total_memory: u64,
    used_memory: u64,
}

impl FromRedisValue for Node {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let json_str: String = redis::FromRedisValue::from_redis_value(v)?;
        serde_json::from_str(&json_str).map_err(|_| {
            RedisError::from((
                redis::ErrorKind::TypeError,
                "Failed to parse Node from JSON",
            ))
        })
    }
}

#[derive(Debug, Deserialize, Clone)]
struct Room {
    address: String,
    node_id: String,
    participants: u32,
}

impl FromRedisValue for Room {
    fn from_redis_value(v: &redis::Value) -> redis::RedisResult<Self> {
        let json_str: String = redis::FromRedisValue::from_redis_value(v)?;
        serde_json::from_str(&json_str).map_err(|_| {
            RedisError::from((
                redis::ErrorKind::TypeError,
                "Failed to parse Room from JSON",
            ))
        })
    }
}

// Enum for Tabs
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Page {
    Nodes,
    Rooms,
}

impl Page {
    fn next(&self) -> Self {
        match self {
            Page::Nodes => Page::Rooms,
            Page::Rooms => Page::Nodes,
        }
    }

    fn titles() -> Vec<&'static str> {
        vec!["Nodes", "Rooms"]
    }

    fn index(&self) -> usize {
        match self {
            Page::Nodes => 0,
            Page::Rooms => 1,
        }
    }
}

// Event enum for handling input and tick events
enum Event<I> {
    Input(I),
    Tick,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Load environment variables from .env
    dotenv().ok();
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create a channel to receive events
    let (tx, mut rx) = mpsc::channel::<Event<CEvent>>(100);

    // Spawn a task to handle input events
    tokio::spawn(async move {
        loop {
            // Poll for input events
            if event::poll(Duration::from_millis(100)).unwrap() {
                if let Ok(ev) = event::read() {
                    tx.send(Event::Input(ev)).await.unwrap();
                }
            }
            // Send tick event
            tx.send(Event::Tick).await.unwrap();
            time::sleep(Duration::from_millis(250)).await;
        }
    });

    // Initialize application state
    let mut page = Page::Nodes;
    let mut nodes: Vec<(String, Node)> = Vec::new();
    let mut rooms: Vec<(String, Room)> = Vec::new();

    // Redis connection
    let client = redis::Client::open(redis_url)?;
    let mut con = client.get_multiplexed_async_connection().await?;

    // Main loop
    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                match event {
                    Event::Input(input) => {
                        if let CEvent::Key(key) = input {
                            match key.code {
                                KeyCode::Char('q') => break,
                                KeyCode::Tab => {
                                    page = page.next();
                                },
                                KeyCode::Left | KeyCode::Right => {
                                    // Optional: Handle left/right arrow keys for navigation
                                    page = page.next();
                                },
                                _ => {}
                            }
                        }
                    },
                    Event::Tick => {
                        if let Err(e) = update_data(&mut con, &mut nodes, &mut rooms).await {
                            // Handle Redis errors gracefully, e.g., log them
                            eprintln!("Error updating data: {}", e);
                        }
                    },
                }
            }
            else => break,
        }

        // Draw UI
        if let Err(e) = terminal.draw(|f| ui(f, page, &nodes, &rooms)) {
            eprintln!("Error drawing UI: {}", e);
            break;
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}

/// Fetches data from Redis and updates the nodes and rooms vectors.
async fn update_data(
    con: &mut redis::aio::MultiplexedConnection,
    nodes: &mut Vec<(String, Node)>,
    rooms: &mut Vec<(String, Room)>,
) -> Result<(), Box<dyn Error>> {
    // Fetch nodes
    let node_keys: Vec<String> = con.keys("node:*").await?;
    nodes.clear();
    if !node_keys.is_empty() {
        let node_values: Vec<Option<Node>> = con.mget(&node_keys).await?;
        for (key, node_opt) in node_keys.into_iter().zip(node_values.into_iter()) {
            if let Some(node) = node_opt {
                nodes.push((key, node));
            }
        }
    }

    // Fetch rooms
    let room_keys: Vec<String> = con.keys("room:*").await?;
    rooms.clear();
    if !room_keys.is_empty() {
        let room_values: Vec<Option<Room>> = con.mget(&room_keys).await?;
        for (key, room_opt) in room_keys.into_iter().zip(room_values.into_iter()) {
            if let Some(room) = room_opt {
                rooms.push((key, room));
            }
        }
    }

    Ok(())
}

/// Renders the UI based on the current state.
fn ui<B: Backend>(
    f: &mut Frame<B>,
    page: Page,
    nodes: &Vec<(String, Node)>,
    rooms: &Vec<(String, Room)>,
) {
    // Define layout with an additional constraint for the footer
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints(
            [
                Constraint::Length(3), // For Tabs
                Constraint::Min(0),    // For Main Content
                Constraint::Length(1), // For Footer
            ]
            .as_ref(),
        )
        .split(f.size());

    // Tabs
    let titles = Page::titles()
        .iter()
        .map(|t| Spans::from(Span::styled(*t, Style::default().fg(Color::Yellow))))
        .collect();
    let tabs = Tabs::new(titles)
        .select(page.index())
        .block(Block::default().borders(Borders::ALL).title("Pages"))
        .highlight_style(
            Style::default()
                .fg(Color::LightGreen)
                .add_modifier(Modifier::BOLD),
        );
    f.render_widget(tabs, chunks[0]);

    // Content
    match page {
        Page::Nodes => render_nodes(f, chunks[1], nodes),
        Page::Rooms => render_rooms(f, chunks[1], rooms),
    }

    // Footer with exit instruction
    let footer = Paragraph::new("Press 'q' to exit")
        .style(Style::default().fg(Color::White))
        .alignment(tui::layout::Alignment::Center);
    f.render_widget(footer, chunks[2]);
}

/// Renders the Nodes table.
fn render_nodes<B: Backend>(
    f: &mut Frame<B>,
    area: tui::layout::Rect,
    nodes: &Vec<(String, Node)>,
) {
    // Define table headers
    let header = [
        "Key",
        "Address",
        "# Rooms",
        "# Connections",
        "CPU Usage",
        "Total Memory",
        "Used Memory",
    ];

    // Create table rows
    let rows: Vec<Row> = nodes
        .iter()
        .map(|(k, n)| {
            Row::new(vec![
                Cell::from(k.clone()),
                Cell::from(n.address.clone()),
                Cell::from(n.num_rooms.to_string()),
                Cell::from(n.num_connections.to_string()),
                Cell::from(format!("{:.2}%", n.cpu_usage * 100.0)),
                Cell::from(format!("{:.2} GB", n.total_memory as f64 / 1e9)),
                Cell::from(format!("{:.2} GB", n.used_memory as f64 / 1e9)),
            ])
        })
        .collect();

    // Define table
    let table = Table::new(rows)
        .header(
            Row::new(
                header
                    .iter()
                    .map(|h| {
                        Cell::from(*h).style(
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        )
                    })
                    .collect::<Vec<Cell>>(),
            )
            .bottom_margin(1),
        )
        .block(Block::default().borders(Borders::ALL).title("Nodes"))
        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
        .widths(&[
            Constraint::Length(20),
            Constraint::Length(15),
            Constraint::Length(10),
            Constraint::Length(15),
            Constraint::Length(10),
            Constraint::Length(15),
            Constraint::Length(15),
        ]);

    // Render table
    f.render_widget(table, area);
}

/// Renders the Rooms table.
fn render_rooms<B: Backend>(
    f: &mut Frame<B>,
    area: tui::layout::Rect,
    rooms: &Vec<(String, Room)>,
) {
    // Define table headers
    let header = ["Key", "Address", "Node ID", "Participants"];

    // Create table rows
    let rows: Vec<Row> = rooms
        .iter()
        .map(|(k, r)| {
            Row::new(vec![
                Cell::from(k.clone()),
                Cell::from(r.address.clone()),
                Cell::from(r.node_id.clone()),
                Cell::from(r.participants.to_string()),
            ])
        })
        .collect();

    // Define table
    let table = Table::new(rows)
        .header(
            Row::new(
                header
                    .iter()
                    .map(|h| {
                        Cell::from(*h).style(
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        )
                    })
                    .collect::<Vec<Cell>>(),
            )
            .bottom_margin(1),
        )
        .block(Block::default().borders(Borders::ALL).title("Rooms"))
        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
        .widths(&[
            Constraint::Length(20),
            Constraint::Length(15),
            Constraint::Length(25),
            Constraint::Length(15),
        ]);

    // Render table
    f.render_widget(table, area);
}
