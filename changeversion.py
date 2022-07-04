
def change_version():
    file = 'setup.py'
    prefix = 'VNU'
    file_data = ""
    row_num = 0
    with open(file, "r", encoding="utf-8") as fr:
        for line in fr:
            row_num += 1
            if row_num in range(1, 5) and prefix in line:
                vnu = int(line.split('=')[1]) + 1
                line = "{} = {}\n".format(prefix, vnu)
            file_data += line
    with open(file, "w", encoding="utf-8") as fw:
        fw.write(file_data)


def main():
    change_version()


if __name__ == '__main__':
    main()