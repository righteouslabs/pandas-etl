def pytest_addoption(parser):
    parser.addoption(
        "--file",
        help="Path to yaml config file",
        metavar="'usr/dir/file.yaml'",
        default=None,
    )
    parser.addoption(
        "--imports",
        action="extend",
        nargs="*",
        help="Add or override imports",
        metavar="'./vars/sql-db1.yaml'",
        default=None,
    )
    parser.addoption(
        "--var",
        action="extend",
        nargs="*",
        help="Define new variables or overwrite existing variables",
        metavar="varName1=varValue1",
        default=None,
    )
