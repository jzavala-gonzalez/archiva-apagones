from datetime import datetime, timezone, timedelta

def get_atc_now():
    """Get current time in AST (GMT-4)"""
    # Get current UTC time
    utc_now = datetime.utcnow()

    # Define the AST timezone offset (4 hours behind UTC)
    ast_offset = timedelta(hours=-4)

    # Create a timezone object for AST (GMT-4)
    ast_tz = timezone(ast_offset)

    # Apply the AST timezone offset to the current UTC time
    ast_time = utc_now.replace(tzinfo=timezone.utc).astimezone(ast_tz)

    return ast_time
