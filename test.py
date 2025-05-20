def authorizeAdmin(usr):

    assert isinstance(usr, list) and usr != [], "No user found"

    assert 'admin' in usr, "No admin found."

    print("You are granted full access to the application.")


if __name__ == '__main__':

    authorizeAdmin(['user'])