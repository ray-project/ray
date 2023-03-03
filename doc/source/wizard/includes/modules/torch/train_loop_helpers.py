from torchvision import models


def build_model():
    # Load pretrained model params
    model = models.efficientnet_b0(pretrained=True)

    # Replace the original classifier with a new Linear layer
    num_features = model.classifier[1].in_features
    model.classifier[1] = nn.Linear(num_features, 10)

    for param in model.parameters():
        param.requires_grad = True
    return model
