import os
import importlib
import argparse

# import check


def get_args():

    parser = argparse.ArgumentParser(
        description="Data transformer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--urlc", help="unique url code", required=True)

    args = parser.parse_args()

    return args

def module_loader(module_path):
    """
    Load module
    :param module_path: complete package structure name
    :type module_name: str
    :return: module_pointer
    :rtype: module
    >>> module_loader('pg.teal***.dl_dc_orders.clean') ->
    <class module>
    """

    module_pointer = importlib.import_module(module_path)

    return module_pointer


def main():
    # args = get_args()
    # urlc  = args.urlc
    # module_path = 'new_transformer.urlc.mh_gom_rd_deeds_pune.pune_deeds_regular' + urlc
    print(os.getcwd())
    print(os.listdir())

    print('--------------------------')
    # module_path = 'new_transformer.urlc.test.check'
    module_path = 'new_transformer.urlc.mh_gom_rd_deeds_pune.pune_deeds_regular'
    module = module_loader(module_path)
    module.main()
    # # module = os.path.splitext(filename)[0]
    # print(module)
    # module = importlib.import_module(module)
    # module.main()


if __name__ == '__main__':
    main()
    