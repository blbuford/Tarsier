use std::io;

fn main() {
    let mut input = String::new();
    loop {
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                if input.starts_with(".exit") {
                    return;
                } else {
                    input.pop();
                    println!("Unrecognized command: {}", input);
                }
            }
            Err(why) => println!("Error: {why}"),
        }
        input.clear()
    }
}
