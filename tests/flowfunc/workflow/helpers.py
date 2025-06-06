def md5_hash(text_to_be_hashed):
    import hashlib

    return hashlib.md5(str(text_to_be_hashed).encode("utf-8")).hexdigest()


def markdown_make_bold(text_to_be_made_bold) -> str:
    return f"**{text_to_be_made_bold}**"
