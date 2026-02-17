import prompt

def welcome() -> None:
    print("Первая попытка запустить проект!")
    print()
    print("***")
    print("<command> exit - выйти из программы")
    print("<command> help - справочная информация")

    command = prompt.string("Введите команду: ")
    if command == "help":
        print()
        print("<command> exit - выйти из программы")
        print("<command> help - справочная информация")
        prompt.string("Введите команду: ")
