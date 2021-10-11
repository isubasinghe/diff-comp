use difflouvain_utils::utils::*;

fn main() {
    let source = std::env::args().nth(1).unwrap();
    let sink = std::env::args().nth(2).unwrap();
    println!("Processing");
    println!("Source {}", source);
    println!("Sink {}", sink);
    read_file_processing(&source, &sink);
}
