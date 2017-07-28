from __future__ import print_function
import json
import jsonschema
import glob
from . import resources

class Input(object):
    _subtypes = None
    @classmethod
    def Deserialise(cls, data):
        assert isinstance(data, dict)
        type_str = data['type']
        
        if cls._subtypes is None:
            cls._subtypes = {subcls.TAG: subcls for subcls in cls.__subclasses__()}
            
        child_cls = cls._subtypes[type_str]
        return child_cls.Deserialise(data)
    pass

class InputFile(Input):
    TAG = 'file'
    @classmethod
    def Deserialise(cls, data):
        ans = cls()
        ans.path = data['path']
        return ans

    def process(self, uploader):
        blob = uploader.file(self.path)
        return ["curl '{}' > {}\n".format(blob.url, self.path)]
    
    pass

class InputPattern(Input):
    TAG = 'pattern'
    @classmethod
    def Deserialise(cls, data):
        ans = cls()
        ans.pattern = data['pattern']
        return ans

    def process(self, uploader):
        commands = []
        for f_path in glob.iglob(self.pattern):
            blob = uploader.file(f_path)
            commands.append("curl '{}' > {}\n".format(blob.url, f_path))
        if len(commands) == 0:
            print("Did not upload anything for pattern '{}'".format(self.pattern))
        return commands
    
    pass

class Command(object):
    _subtypes = None
    
    REDIRECT_TEMPLATE = ' > {index}.out 2> {index}.err'
    
    @classmethod
    def Deserialise(cls, data):
        assert isinstance(data, dict)
        type_str = data['type']
        if cls._subtypes is None:
            cls._subtypes = {subcls.TAG: subcls for subcls in cls.__subclasses__()}
            
        child_cls = cls._subtypes[type_str]
        ans = child_cls.Deserialise(data)
        ans.redirect = data.get('redirect', False)
        return ans

    def suffix(self, index):
        if self.redirect:
            return self.REDIRECT_TEMPLATE.format(index=index)
        else:
            return ''
        
    def process(self, index):
        cmds = [self.PREFIX + self.expression + self.suffix(index)]
        if self.redirect:
            cmds.append('upld {index}.out'.format(index=index))
            cmds.append('upld {index}.err'.format(index=index))
        return cmds
    pass

class SerialCommand(Command):
    TAG = 'serial'
    PREFIX = ''
    @classmethod
    def Deserialise(cls, data):
        ans = cls()
        ans.expression = data['expression']
        return ans
    pass

class ParallelCommand(Command):
    TAG = 'parallel'
    PREFIX = 'mpirun -np $num_cores -ppn $cores_per_node -hosts $AZ_BATCH_HOST_LIST '
    @classmethod
    def Deserialise(cls, data):
        ans = cls()
        ans.expression = data['expression']
        return ans
    pass

class Output(object):
    _subtypes = None
    @classmethod
    def Deserialise(cls, data):
        assert isinstance(data, dict)
        type_str = data['type']
        
        if cls._subtypes is None:
            cls._subtypes = {subcls.TAG: subcls for subcls in cls.__subclasses__()}
            
        child_cls = cls._subtypes[type_str]
        return child_cls.Deserialise(data)
    pass

class OutputFile(Output):
    TAG = 'file'
    @classmethod
    def Deserialise(cls, data):
        ans = cls()
        ans.path = data['path']
        return ans
    
    def process(self):
        return ['upld ' + self.path]
    
    pass

class OutputPattern(Output):
    TAG = 'pattern'
    @classmethod
    def Deserialise(cls, data):
        ans = cls()
        ans.pattern = data['pattern']
        return ans
    
    def process(self):
        return ['for output in {}; do'.format(self.pattern),
                'upld $output',
                'done']
    
    pass

class JobSpec(object):
    with open(resources.get('batch', 'job_spec.json')) as sf:
        schema = json.load(sf)
    del sf
    
    def __init__(self, filename):
        with open(filename) as f:
            js = json.load(f)
        jsonschema.validate(js, self.schema)
        
        self.name = js['name']
        self.inputs = [Input.Deserialise(i) for i in js['inputs']]
        self.commands = [Command.Deserialise(c) for c in js['commands']]        
        self.outputs = [Output.Deserialise(o) for o in js['outputs']]
        
        return
    pass
