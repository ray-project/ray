from PIL import ImageFile


def type_to_string(_type):
    if isinstance(_type, type) and issubclass(_type, ImageFile.ImageFile):
        return str(ImageFile.ImageFile)

    return str(_type)
