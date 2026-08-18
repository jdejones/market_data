# AGENTS.md

## Review guidelines

- During **automated pull request reviews** and **Code Reviews** (e.g., Codex Code Review, CI bots),
  do not modify code or open new commits; only leave comments and suggestions. There are no exceptions unless the user explicitly states "you may edit the code."
- During **interactive IDE sessions**, you may edit code when the user explicitly
  asks for changes or confirms a proposed diff.

## Database Storage Assumtions
- Database Management System is MySQL
- SQLAlchemy is used to access MySQL
- Assume the IDE has an MCP connected that can access the database.
### Credentials
- hostname and port: 127.0.0.1:3306
- password in api_keys.py
