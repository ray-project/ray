class TrainerError(Exception):
    pass


class TrainerConfigError(TrainerError, ValueError):
    pass
