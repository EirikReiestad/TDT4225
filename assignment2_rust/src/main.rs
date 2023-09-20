use example::ExampleProgram;

fn main() {
    let program = ExampleProgram::new();
    program.create_table("Person");
    program.insert_data("Person");
    program.fetch_data("Person");
    program.drop_table("Person");
    program.show_tables();
}
