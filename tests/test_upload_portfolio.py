import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dashboard.upload_portfolio import clone_portfolio, generate_input_template, save_portfolio_to_template


def cmd_generate_template(args):
    print(f'Generating template: account_id={args.account_id}, filename={args.filename} ...')
    try:
        positions, params, limit, client_id = generate_input_template(args.account_id)
        out_path = save_portfolio_to_template(positions, params, limit, client_id, args.filename)
    except Exception as e:
        print(f'Error: {e}')
        sys.exit(1)

    if out_path.exists():
        size_kb = out_path.stat().st_size / 1024
        print(f'OK — file saved: {out_path} ({size_kb:.1f} KB)')
    else:
        print(f'Error: function returned {out_path} but file does not exist')
        sys.exit(1)


_MOCK_TARGET_WEIGHTS = {
    'fi':  40,
    'eq':  35,
    'alt': 10,
    'ma':   5,
    'mm':  10,
}

def cmd_clone(args):
    print(f'Cloning portfolio: port_id={args.port_id}, new_port_name="{args.port_name}", username={args.username} ...')
    print(f'Target weights: {_MOCK_TARGET_WEIGHTS}')
    try:
        new_port_id = clone_portfolio(args.port_id, args.port_name, args.username,
                                      target_weights=_MOCK_TARGET_WEIGHTS, background=False)
    except Exception as e:
        print(f'Error: {e}')
        sys.exit(1)

    print(f'OK — cloned to new port_id={new_port_id}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test upload_portfolio functions.')
    sub = parser.add_subparsers(dest='command', required=True)

    p_gen = sub.add_parser('generate-template', help='Test generate_input_template()')
    p_gen.add_argument('--account-id', type=int, default=1003, metavar='ACCOUNT_ID')
    p_gen.add_argument('--filename',   type=str, default='test_template_output.xlsx', metavar='FILENAME')

    p_clone = sub.add_parser('clone', help='Test clone_portfolio()')
    p_clone.add_argument('--port-id',       type=int, required=True,           metavar='PORT_ID')
    p_clone.add_argument('--port-name', type=str, required=True,           metavar='NEW_PORT_NAME')
    p_clone.add_argument('--username',      type=str, required=True,           metavar='USERNAME')

    args = parser.parse_args()

    if args.command == 'generate-template':
        cmd_generate_template(args)
    elif args.command == 'clone':
        cmd_clone(args)
