def message(role: str, message: str, **kwargs):
    if "image_urls" in kwargs:
        image_urls = kwargs.pop("image_urls")
        content = [
            {
                "type": "text",
                "text": message,
            },
        ]
        for image_url in image_urls:
            content.append(
                {
                    "type": "image_url",
                    "image_url": {
                        "url": image_url,
                    },
                }
            )
        return {"role": role, "content": content, **kwargs}

    return {"role": role, "content": message, **kwargs}


def system(msg: str, **kwargs):
    return message("system", msg, **kwargs)


def user(msg: str, **kwargs):
    return message("user", msg, **kwargs)


def tool(msg: str, **kwargs):
    return message("tool", msg, **kwargs)


def messages(*messages, **kwargs):
    return {"messages": list(messages), **kwargs}


def prompt(message: str):
    return {"prompt": message}


def input(message: str):
    return {"input": message}
