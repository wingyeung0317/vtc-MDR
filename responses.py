def handle_response(msg) -> str:
    p_msg = msg.lower()
    
    if p_msg == "/print":
        return "test"