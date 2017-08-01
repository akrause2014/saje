from __future__ import print_function
import json
import jsonschema
import glob
import hashlib

from . import resources

class Input(object):
    _subtypes = None
    @classmethod
    def FromJson(cls, data):
        assert isinstance(data, dict)
        type_str = data['type']
        
        if cls._subtypes is None:
            cls._subtypes = {subcls.TAG: subcls for subcls in cls.__subclasses__()}
            
        child_cls = cls._subtypes[type_str]
        return child_cls.FromJson(data)
    
    def ToJson(self):
        return {u'type': self.TAG}

    pass

class InputFile(Input):
    TAG = u'file'
    @classmethod
    def FromJson(cls, data):
        ans = cls()
        ans.path = data['path']
        return ans

    def apply(self, func):
        return [func(self.path)]

    def ToJson(self):
        ans = super(InputFile, self).ToJson()
        ans[u'path'] = self.path
        return ans
    
    pass

class InputPattern(Input):
    TAG = u'pattern'
    @classmethod
    def FromJson(cls, data):
        ans = cls()
        ans.pattern = data['pattern']
        return ans

    def apply(self, func):
        return [func(f_path)for f_path in glob.iglob(self.pattern)]
    
    def ToJson(self):
        ans = super(InputPattern, self).ToJson()
        ans[u'pattern'] = self.pattern
        return ans

    pass

class Command(object):
    _subtypes = None
    
    REDIRECT_TEMPLATE = ' > {index}.out 2> {index}.err'
    
    @classmethod
    def FromJson(cls, data):
        assert isinstance(data, dict)
        type_str = data['type']
        if cls._subtypes is None:
            cls._subtypes = {subcls.TAG: subcls for subcls in cls.__subclasses__()}
            
        child_cls = cls._subtypes[type_str]
        ans = child_cls.FromJson(data)
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
    
    def ToJson(self):
        return {u'type': self.TAG,
                u'redirect': self.redirect}
    pass

class SerialCommand(Command):
    TAG = u'serial'
    PREFIX = ''
    @classmethod
    def FromJson(cls, data):
        ans = cls()
        ans.expression = data['expression']
        return ans
    
    def ToJson(self):
        ans = super(SerialCommand, self).ToJson()
        ans[u'expression'] = self.expression
        return ans
    pass

class ParallelCommand(Command):
    TAG = u'parallel'
    PREFIX = 'mpirun -np $num_cores -ppn $cores_per_node -hosts $AZ_BATCH_HOST_LIST '
    @classmethod
    def FromJson(cls, data):
        ans = cls()
        ans.expression = data['expression']
        return ans
    
    def ToJson(self):
        ans = super(ParallelCommand, self).ToJson()
        ans[u'expression'] = self.expression
        return ans
    pass

class Output(object):
    _subtypes = None
    @classmethod
    def FromJson(cls, data):
        assert isinstance(data, dict)
        type_str = data['type']
        
        if cls._subtypes is None:
            cls._subtypes = {subcls.TAG: subcls for subcls in cls.__subclasses__()}
            
        child_cls = cls._subtypes[type_str]
        return child_cls.FromJson(data)
    
    def ToJson(self):
        return {u'type': self.TAG}
    
    pass

class OutputFile(Output):
    TAG = u'file'
    @classmethod
    def FromJson(cls, data):
        ans = cls()
        ans.path = data['path']
        return ans
    
    def process(self):
        return ['upld ' + self.path]
    
    def ToJson(self):
        ans = super(OutputFile, self).ToJson()
        ans[u'path'] = self.path
        return ans
    
    pass

class OutputPattern(Output):
    TAG = u'pattern'
    @classmethod
    def FromJson(cls, data):
        ans = cls()
        ans.pattern = data['pattern']
        return ans
    
    def process(self):
        return ['for output in {}; do'.format(self.pattern),
                'upld $output',
                'done']
    
    def ToJson(self):
        ans = super(OutputPattern, self).ToJson()
        ans[u'pattern'] = self.pattern
        return ans
    
    pass

class JobSpec(object):
    with open(resources.get('batch', 'job_spec.json')) as sf:
        schema = json.load(sf)
    del sf

    @classmethod
    def FromJson(cls, data):
        ans = cls()
        ans.name = data['name']
        ans.inputs = [Input.FromJson(i) for i in data['inputs']]
        ans.commands = [Command.FromJson(c) for c in data['commands']]        
        ans.outputs = [Output.FromJson(o) for o in data['outputs']]
        return ans
    
    @classmethod
    def open(cls, filename):
        with open(filename) as f:
            js = json.load(f)
        jsonschema.validate(js, cls.schema)
                
        return cls.FromJson(js)

    def ToJson(self):
        ans = {}
        ans[u'name'] = self.name
        ans[u'inputs'] = [i.ToJson() for i in self.inputs]
        ans[u'commands'] = [c.ToJson() for c in self.commands]
        ans[u'outputs'] = [o.ToJson() for o in self.outputs]
        jsonschema.validate(ans, self.schema)
        return ans        
    pass

def ReproducibleHash(jsObj):
    sha1 = hashlib.sha1()
    
    if isinstance(jsObj, dict):
        keys = sorted(jsObj.keys())
        for k in keys:
            sha1.update(k)
            hv = ReproducibleHash(jsObj[k])
            sha1.update(hv)
    elif isinstance(jsObj, list):
        for i in jsObj:
            sha1.update(ReproducibleHash(i))
    elif isinstance(jsObj, (str, unicode)):
        sha1.update(jsObj)
    elif isinstance(jsObj, (int, float, bool, type(None))):
        sha1.update(str(jsObj))
    else:
        raise TypeError("Type '{}' isn't in json model so don't know how to hash".format(type(jsObj)))
    
    return sha1.hexdigest()
