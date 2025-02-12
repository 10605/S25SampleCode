import collections

PROGRAM = [('z1', 'add', ['x1', 'x1']),
           ('z2', 'add', ['z1', 'x2']),
           ('f', 'square', ['z2'])]

G = {
    'add':      lambda x1,x2: x1 + x2,
    'square':   lambda x: x * x
}

DG = {
    'add': [lambda x1,x2: 1, lambda x1,x2: 1],
    'square': [lambda x: 2.0 * x],
}


def forward(opseq, val):
    for output, fn_name, inputs in opseq:
        input_vals = [val[inp] for inp in inputs]
        fn = G[fn_name]
        val[output] = fn(*input_vals)
        print(f'val[{output}]={val[output]}')

def backward(opseq, val, delta):
    for output, fn_name, inputs in reversed(opseq):
        input_vals = [val[inp] for inp in inputs]
        print(f'{output=} {fn_name=} {inputs=}')
        for i in range(len(inputs)):
            xi = inputs[i]
            dxi_dinputi_fn = DG[fn_name][i]
            dy_dxi = delta[output] * dxi_dinputi_fn(*input_vals)
            delta[xi] += dy_dxi
            print(f'delta[{xi}] += {dy_dxi}')

if __name__ == '__main__':
    val = dict(x1=2, x2=3)
    forward(PROGRAM, val) 
    print(f'{val=}')
    delta = collections.defaultdict(float)
    delta['f'] = 1.0
    backward(PROGRAM, val, delta)
    print(f'{delta=}')

    
