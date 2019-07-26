from pip._internal.operations.freeze import freeze
for requirement in freeze(local_only=True):
    print(requirement)