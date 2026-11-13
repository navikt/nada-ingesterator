build:
	cargo build --release

diagrams:
	plantuml --svg "docs/*.puml"
