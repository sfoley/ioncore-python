#!/usr/bin/env python
"""
@file ion/core/object/gpb_wrapper.py
@brief Wrapper for Google Protocol Buffer Message Classes.
These classes are the lowest level of the object management stack
@author David Stuebe
@author 
TODO:
Finish test of new Invalid methods using weakrefs - make sure it is deleted!
"""

from ion.core.object.object_utils import get_type_from_obj, sha1bin, sha1hex, \
    sha1_to_hex, ObjectUtilException, create_type_identifier, get_gpb_class_from_type_id

import struct

from google.protobuf import message
from google.protobuf.internal import containers
from google.protobuf import descriptor

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)



STRUCTURE_ELEMENT_TYPE = create_type_identifier(object_id=1, version=1)
LINK_TYPE = create_type_identifier(object_id=3, version=1)

# Types which require specialization!
CDM_DATASET_TYPE = create_type_identifier(object_id=10001, version=1)
CDM_VARIABLE_TYPE = create_type_identifier(object_id=10024, version=1)
CDM_GROUP_TYPE = create_type_identifier(object_id=10020, version=1)
CDM_DIMENSION_TYPE = create_type_identifier(object_id=10018, version=1)
CDM_ATTRIBUTE_TYPE = create_type_identifier(object_id=10017, version=1)
CDM_ARRAY_INT32_TYPE = create_type_identifier(object_id=10009, version=1)
CDM_ARRAY_UINT32_TYPE = create_type_identifier(object_id=10010, version=1)
CDM_ARRAY_INT64_TYPE = create_type_identifier(object_id=10011, version=1)
CDM_ARRAY_FLOAT32_TYPE = create_type_identifier(object_id=10013, version=1)
CDM_ARRAY_FLOAT64_TYPE = create_type_identifier(object_id=10014, version=1)
CDM_ARRAY_STRING_TYPE = create_type_identifier(object_id=10015, version=1)
CDM_ARRAY_OPAQUE_TYPE = create_type_identifier(object_id=10016, version=1)

class OOIObjectError(Exception):
    """
    An exception class for errors that occur in the Object Wrapper class
    """
    
class WrappedEnum(object):
    """ Data descriptor (like a property) for passing through GPB enums from the Wrapper. """

    def __init__(self, val, doc=None):
        self.val = val
        if doc: self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        return self.val

    def __set__(self, obj, value):
        raise AttributeError('Enums are read-only.')
        
    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Wrapper property for an ION Object field')

class EnumType(type):
    """
    Metaclass that automatically generates subclasses of Wrapper with corresponding enums and
    pass-through properties for each field in the protobuf descriptor.
    """

    _type_cache = {}

    def __call__(cls, enum_type_descriptor, *args, **kwargs):
        # Cache the custom-built classes
        
        assert isinstance(enum_type_descriptor, descriptor.EnumDescriptor)
            
        clsType = None
        enum_name = enum_type_descriptor.name
        enum_full_name = enum_type_descriptor.full_name
        
        if enum_full_name in EnumType._type_cache:
            clsType = EnumType._type_cache[enum_full_name]
        else:
            clsName = '%s_%s' % (cls.__name__, enum_name)
            clsDict = {}


            for name, val_desc in enum_type_descriptor.values_by_name.items():
                
                prop = WrappedEnum(val_desc.number)
                
                clsDict[name] = prop

            clsType = EnumType.__new__(EnumType, clsName, (cls,), clsDict)

            # Also set the enum descriptors _after_ building the class so the descriptor doesn't go away
            #if hasattr(descriptor, 'enum_values_by_name'):
            #    for k,v in descriptor.enum_values_by_name.iteritems():
            #        setattr(clsType, k, WrappedEnum(v.number))

            EnumType._type_cache[enum_full_name] = clsType

        # Finally allow the instantiation to occur, but slip in our new class type
        obj = super(EnumType, clsType).__call__(enum_type_descriptor, *args, **kwargs)
        return obj

class EnumObject(object):
    '''
    A Class for GPB Enum access
    '''

    __metaclass__ = EnumType
                
    def __init__(self, enum_type_descriptor):
        """
        Instantiate a class with properties to get GPB Enum Values
        """

    

class WrappedProperty(object):
    
    def __init__(self, name, doc=None, field_type=None, field_enum=None):
        self.name = name
        if doc: self.__doc__ = doc

        if field_type is None:
            pass
        elif field_type == 8:
            self.field_type = "TYPE_BOOL"
        elif field_type == 12:
            self.field_type = "TYPE_BYTES"
        elif field_type == 1:
            self.field_type = "TYPE_DOUBLE"
        elif field_type == 14:
            self.field_type = "TYPE_ENUM"
        elif field_type == 7:
            self.field_type = "TYPE_FIXED32"
        elif field_type == 6:
            self.field_type = "TYPE_FIXED64"
        elif field_type == 2:
            self.field_type = "TYPE_FLOAT"
        elif field_type == 10:
            self.field_type = "TYPE_GROUP"
        elif field_type == 5:
            self.field_type = "TYPE_INT32"
        elif field_type == 3:
            self.field_type = "TYPE_INT64"
        elif field_type == 2:
            self.field_type = "TYPE_FLOAT"
        elif field_type == 11:
            self.field_type = 'TYPE_MESSAGE'
        elif field_type == 15:
            self.field_type = 'TYPE_SFIXED32'
        elif field_type == 16:
            self.field_type = 'TYPE_SFIXED64'
        elif field_type == 17:
            self.field_type = 'TYPE_SINT32'
        elif field_type == 18:
            self.field_type = 'TYPE_SINT64'
        elif field_type == 9:
            self.field_type = 'TYPE_STRING'
        elif field_type == 13:
            self.field_type = 'TYPE_UINT32'
        elif field_type == 4:
            self.field_type = 'TYPE_UINT64'

        self.field_enum = field_enum

    def __get__(self, wrapper, objtype=None):
        raise NotImplementedError('Abstract base class for property wrappers: __get__')

    def __set__(self, wrapper, value):
        raise NotImplementedError('Abstract base class for property wrappers: __set__')
        
    def __delete__(self, wrapper):
        raise NotImplementedError('Abstrat base class for property wrappers: __delete__')


class WrappedMessageProperty(WrappedProperty):
    """ Data descriptor (like a property) for passing through GPB properties of Type Message from the Wrapper. """
        
    def __get__(self, wrapper, objtype=None):
        if wrapper._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        # This may be the result we were looking for, in the case of a simple scalar field
        field = getattr(wrapper.GPBMessage, self.name)
        result = wrapper._rewrap(field)

        if result.ObjectType == LINK_TYPE:
            result = wrapper.Repository.get_linked_object(result)

        return result

    def __set__(self, wrapper, value):
        if wrapper._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if wrapper.ReadOnly:
            raise OOIObjectError('This object wrapper is read only!')

        # get the callable and call it!
        wrapper.SetLinkByName(self.name, value)
        wrapper._set_parents_modified()

        return None
    
    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Wrapper property for an ION Object field')

class WrappedRepeatedScalarProperty(WrappedProperty):
    """ Data descriptor (like a property) for passing through GPB properties of Type Repeated Scalar from the Wrapper. """
        
    def __get__(self, wrapper, objtype=None):
        if wrapper._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        # This may be the result we were looking for, in the case of a simple scalar field
        field = getattr(wrapper.GPBMessage, self.name)
        
        return ScalarContainerWrapper.factory(wrapper, field)

    def __set__(self, wrapper, value):
        raise AttributeError('Assignment is not allowed for field name "%s" of type Repeated Scalar in ION Object')

        return None
    
    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Wrapper property for an ION Object field')

class WrappedRepeatedCompositeProperty(WrappedProperty):
    """ Data descriptor (like a property) for passing through GPB properties of Type Repeated Composite from the Wrapper. """
        
    def __get__(self, wrapper, objtype=None):
        if wrapper._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        # This may be the result we were looking for, in the case of a simple scalar field
        field = getattr(wrapper.GPBMessage, self.name)

        return ContainerWrapper.factory(wrapper, field)
        
    def __set__(self, wrapper, value):
        raise AttributeError('Assignement is not allowed for field name "%s" of type Repeated Composite in ION Object')

        return None
    
    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Wrapper property for an ION Object field')

class WrappedScalarProperty(WrappedProperty):
    """ Data descriptor (like a property) for passing through GPB properties of Type Scalar from the Wrapper. """
        
    def __get__(self, wrapper, objtype=None):
        if wrapper._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        # This may be the result we were looking for, in the case of a simple scalar field
        return getattr(wrapper.GPBMessage, self.name)

    def __set__(self, wrapper, value):
        if wrapper._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if wrapper.ReadOnly:
            raise OOIObjectError('This object wrapper is read only!')

        setattr(wrapper.GPBMessage, self.name, value)

        # Set this object and it parents to be modified
        wrapper._set_parents_modified()

        return None
    
    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Wrapper property for an ION Object field')


class CommitCounter(object):
    """
    Class used to count the number of recursive calls to commit a data structure
    """
    count = 0

class WrapperType(type):
    """
    Metaclass that automatically generates subclasses of Wrapper with corresponding enums and
    pass-through properties for each field in the protobuf descriptor.
    
    This approach is generally applicable to wrap data structures. It is extremely powerful!
    """

    _type_cache = {}


    recurse_counter = CommitCounter()


    def __call__(cls, gpbMessage, *args, **kwargs):
        # Cache the custom-built classes
        msgType, clsType = type(gpbMessage), None

        if msgType in WrapperType._type_cache:
            clsType = WrapperType._type_cache[msgType]
        else:

            # Check that the object we are wrapping is a Google Message object
            if not isinstance(gpbMessage, message.Message):
                raise OOIObjectError('Wrapper init argument must be an instance of a GPB message')

            # Get the class name
            clsName = '%s_%s' % (cls.__name__, msgType.__name__)
            clsDict = {}
            properties = {}
            enums = {}

            clsDict['_GPBClass'] = gpbMessage.__class__
            clsDict['_Properties'] = properties
            clsDict['_Enums'] = enums

            clsDict['recurse_count'] = WrapperType.recurse_counter

            # Now setup the properties to map through to the GPB object
            descriptor = msgType.DESCRIPTOR

            # Add the enums of the message class
            if hasattr(descriptor, 'enum_types_by_name'):
                for enum_name, enum_desc in descriptor.enum_types_by_name.iteritems():
                    enum_obj            = EnumObject(enum_desc)
                    clsDict[enum_name]  = enum_obj
                    enums[enum_name]    = enum_obj

            # Add the property wrappers for each of the fields of the message
            for fieldName, field_desc in descriptor.fields_by_name.items():
                fieldType = getattr(msgType, fieldName)

                 # Add any enums for the fields the message contains
                enum_desc = field_desc.enum_type
                field_enum = None
                if enum_desc:
                    field_enum              = EnumObject(enum_desc)
                    clsDict[enum_desc.name] = field_enum
                    enums[enum_desc.name]   = field_enum

                field_type = field_desc.type

                prop = None
                if field_desc.label == field_desc.LABEL_REPEATED:
                    if field_desc.cpp_type == field_desc.CPPTYPE_MESSAGE:
                        prop = WrappedRepeatedCompositeProperty(fieldName, doc=fieldType.__doc__, field_type=field_type, field_enum=field_enum)
                    else:
                        prop = WrappedRepeatedScalarProperty(fieldName, doc=fieldType.__doc__, field_type=field_type, field_enum=field_enum)
                else:
                    if field_desc.cpp_type == field_desc.CPPTYPE_MESSAGE:
                        prop = WrappedMessageProperty(fieldName, doc=fieldType.__doc__, field_type=field_type, field_enum=field_enum)
                    else:
                        prop = WrappedScalarProperty(fieldName, doc=fieldType.__doc__, field_type=field_type, field_enum=field_enum)

                clsDict[fieldName] = prop
                properties[fieldName] = prop

            # Set the object type:
            if clsDict.has_key('_MessageTypeIdentifier'):
                mti = clsDict['_MessageTypeIdentifier']
                obj_type = create_type_identifier(object_id=mti._ID,\
                                                version=mti._VERSION)
            else:
                obj_type = create_type_identifier(object_id=-99,\
                                                version=1)
            clsDict['_gpb_type'] = obj_type
            # the obj_type can now be used for adding special methods to the Wrapper for certain types

            # Special methods for certain object types:
            WrapperType._add_specializations(cls, obj_type, clsDict)

            VALIDATE_ATTRS = False
            if VALIDATE_ATTRS:
                def obj_setter(self, k, v):
                    if self._init and not hasattr(self, k):
                        raise AttributeError(\
                            '''Cant add properties to the ION object wrapper for object Class "%s".\n'''
                            '''Unknown property name - "%s"; value - "%s"''' % (self._GPBClass, k, v))
                    super(Wrapper, self).__setattr__(k, v)

                clsDict['__setattr__'] = obj_setter

            clsDict['_init'] = False


            clsType = WrapperType.__new__(WrapperType, clsName, (cls,), clsDict)

            WrapperType._type_cache[msgType] = clsType

        # Finally allow the instantiation to occur, but slip in our new class type
        obj = super(WrapperType, clsType).__call__(gpbMessage, *args, **kwargs)


        return obj



    def _add_specializations(cls, obj_type, clsDict):

        #-------------------------------------#
        # Wrapper_Dataset Specialized Methods #
        #-------------------------------------#
        def _make_root_group(self, name=''):
            """
            Specialized method for CDM (dataset) Objects to append a group object with the given name
            """
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if self.root_group is not None:
                raise OOIObjectError('Cannot make the root group when one already exists!')

            group = self.Repository.create_object(CDM_GROUP_TYPE)
            group.name = name
            self.root_group = group


        #-----------------------------------#
        # Wrapper_Group Specialized Methods #
        #-----------------------------------#
        def _add_group_to_group(self, name=''):
            """
            Specialized method for CDM (group) Objects to append a group object with the given name
            @return: The group being created (as a convenience)
            """
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name":  Please specify a non-empty string')

            group = self.Repository.create_object(CDM_GROUP_TYPE)
            group.name = name
            group_ref = self.groups.add()
            group_ref.SetLink(group)

            return group


        def _add_attribute(self, name, data_type, values):
            """
            Specialized method for CDM (group) Objects to append a group object with the given name
            @param name: The intented name of the attribute to be appended
            @param values: A List of primitives to be stored as this attributes value.
            @param data_type: A Value from the DataType enum indicating how the 'values' argument should be stored
            @return: The attribute being created (as a convenience)
            """
            # @attention: Should we allow an empty list of values for an attribute?
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')
            if values is None or (isinstance(values, list) and 0 == len(values)):
                raise ValueError('Invalid argument "values" -- Please specify a non-empty list')
            if not data_type or not isinstance(data_type, (int, long)):
                raise TypeError('Type mismatch for argument "data_type" -- Expected int or long; received %s with value: "%s"' % (type(data_type), str(data_type)))

            if not isinstance(values, list):
                values = [values]

            # @todo: all items must be the same type...  this includes ommiting/casting null values
            #        since they will cause an error when stored in the GPB array representation
#            list_type = types.NoneType
#            for item in values:
#                if list_type == types.NoneType:
#                    # Determine the type of values in this list...
#                    list_type = type(item)
#                else:
#                    # ...and ensure all values are the same type or None
#                    next_type = type(item)
#                    if next_type != types.NoneType and next_type != list_type:
#                        raise OOIObjectError('Invalid attribute value list: "%s" -- All items in this list must be of the same type or the value None' % str(values))
#            log.debug('Type of list is "%s" for list: "%s"' % (str(list_type), str(values)))

            # Create the new attribute
            atrib = self.Repository.create_object(CDM_ATTRIBUTE_TYPE)
            atrib.name = name
            atrib.data_type = data_type

            # Set the datatype based on the type of values being given
            # @todo: add support for remaining array types (currently only string attributes are even used)
            # @todo: because many object types are not support fully here -- where should we put value checking...
            #        ex: since bool is stored as int, prevent any value other than 0 and 1
            def _attach_byte_array(parent, atrib_inst):
                atrib_inst.data_type = atrib_inst.DataType.BYTE
                atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_UINT32_TYPE)
            def _attach_short_array(parent, atrib_inst):
                atrib_inst.data_type = atrib_inst.DataType.SHORT
                atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_INT32_TYPE)
            def _attach_int32_array(parent, atrib_inst):
                atrib_inst.data_type = atrib_inst.DataType.INT
                atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_INT32_TYPE)
            def _attach_int64_array(parent, atrib_inst):
                atrib_inst.data_type = atrib_inst.DataType.LONG
                atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_INT64_TYPE)
            def _attach_float32_array(parent, atrib_inst):
                atrib_inst.data_type = atrib_inst.DataType.FLOAT
                atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_FLOAT32_TYPE)
            def _attach_float64_array(parent, atrib_inst):
                atrib_inst.data_type = atrib_inst.DataType.DOUBLE
                atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_FLOAT64_TYPE)
            def _attach_char_array(parent, atrib_inst): # CHAR doesnt exist in GPB
                atrib_inst.data_type = atrib_inst.DataType.CHAR
                atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_STRING_TYPE)
            def _attach_string_array(parent, atrib_inst):
                # @todo: modify this to support unicode types (GPB stringArray already supports it)
                atrib_inst.data_type = atrib_inst.DataType.STRING
                atrib_inst.array = parent.Repository.create_object(CDM_ARRAY_STRING_TYPE)

            attach_array_definitions = {self.DataType.BYTE    : _attach_byte_array,
                                        self.DataType.SHORT   : _attach_short_array,
                                        self.DataType.INT     : _attach_int32_array,
                                        self.DataType.LONG    : _attach_int64_array,
                                        self.DataType.FLOAT   : _attach_float32_array,
                                        self.DataType.DOUBLE  : _attach_float64_array,
                                        self.DataType.CHAR    : _attach_char_array,
                                        self.DataType.STRING  : _attach_string_array}

            attach_array_definitions[data_type](self, atrib)

            # Extend the attribute value array with the given values list
            try:
                atrib.array.value.extend(values)
            except TypeError, ex:
                raise ValueError('Parameter data_type (%s) is incompatible with the given values -- %s' % (str(data_type), str(ex)))

            # Attach the attribute resource instance to its parent resource via CASRef linking
            atrib_ref = self.attributes.add()
            atrib_ref.SetLink(atrib)

            return atrib


        def _add_dimension(self, name, length=-1, variable_length=True):
            """
            Specialized method for CDM Objects to append a dimension object with the given name and length
            @return: The dimension being created (as a convenience)

            @warning: If some component of a dataset contains a dimension: say 'tau', and a dimension is
                      later created anew for a differnt component which does not already contain it, we
                      would effectively have two instances of the same dimension 'tau', which point to
                      different in-memory objects.  This is illegal in this datastructure for dimension objects
                      and should be prevented in further iterations.
            """
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')
            if not isinstance(length, int):
                raise TypeError('Type mismatch for argument "length" -- Expected %s; received %s with value: "%s"' % (repr(int), type(length), str(length)))
            if length <= 0:
                raise ValueError('Invalid argument "dimension": "%s" -- Please specify a positive integer' % str(length))
            if not isinstance(variable_length, bool):
                raise TypeError('Type mismatch for argument "variable_length" -- Expected %s; received %s with value: "%s"' % (type(bool), type(variable_length), str(variable_length)))


            dim = self.Repository.create_object(CDM_DIMENSION_TYPE)
            dim.name = name
            dim.length = length
            dim.variable_length = variable_length
            dim_ref = self.dimensions.add()
            dim_ref.SetLink(dim)

            return dim


        def _add_variable(self, name, data_type, shape=[]):
            """
            Specialized method for CDM Objects to append a variable object with the given name, data_type, and shape
            @return: The variable being created (as a convenience)
            """
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')
            # @todo: Find a better way to ensure DataType is a valid value in cdmdatatype enum
            if not data_type or not isinstance(data_type, int):
                raise TypeError('Type mismatch for argument "data_type" -- Expected %s; received %s with value: "%s"' % (repr(int), type(data_type), str(data_type)))


            var = self.Repository.create_object(CDM_VARIABLE_TYPE)
            var.name = name
            var.data_type = data_type

            # @note: shape is allowed to be null for scalar variables
            if shape is not None:
                if not isinstance(shape, list):
                    raise TypeError('Type mismatch for argument "shape" -- Expected %s; received %s with value: "%s"' % (repr(list), type(shape), str(shape)))
                for dim in shape:
                    if dim is None:
                        raise TypeError('Invalid shape given -- encountered null entries: "%s".  ' % str(shape))
                    elif not hasattr(dim, 'ObjectType'):
                        raise AttributeError('Type mismatch for value in argument "shape" -- Could not access method ObjectType for object "%s" of type "%s"' % (str(dim), type(dim)))
                    elif dim.ObjectType != CDM_DIMENSION_TYPE:
                        raise TypeError('Type mismatch for value in argument "shape" -- Expected %s; received %s with value: "%s"' % (repr(CDM_DIMENSION_TYPE), repr(dim.ObjectType), str(dim)))
                    else:
                        # Add this dimension to the variable's shape!
                        dim_ref = var.shape.add()
                        dim_ref.SetLink(dim)

            var_ref = self.variables.add()
            var_ref.SetLink(var)

            return var


        def _find_group_by_name(self, name=''):
            """
            Specialized method for CDM Objects to find the group object by its name
            """
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

            result = None
            for group in self.groups:
                if group.name == name:
                    result = group
                    break
            if None == result:
                raise OOIObjectError('Requested group name not found: "%s"' % str(name))

            return result


        def _find_attribute_by_name(self, name):
            """
            Specialized method for CDM Objects to find the attribute object by its name
            """
            # @attention: Should this find operate on the variable's standard_name attribute when avail?
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

            result = None
            for att in self.attributes:
                if att.name == name:
                    result = att
                    break
            if None == result:
                raise OOIObjectError('Requested attribute name not found: "%s"' % str(name))

            return result


        def _find_dimension_by_name(self, name):
            """
            Specialized method for CDM Objects to find a dimension object by its name
            """
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

            result = None

            if obj_type == CDM_VARIABLE_TYPE:
                for dim in self.shape:
                    if dim.name == name:
                        result = dim
                        break
            else:
                for dim in self.dimensions:
                    if dim.name == name:
                        result = dim
                        break

            if None == result:
                raise OOIObjectError('Requested dimension name not found: "%s"' % str(name))

            return result


        def _find_variable_by_name(self, name):
            """
            Specialized method for CDM Objects to find the variable object by its name
            """
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

            result = None
            for var in self.variables:
                if var.name == name:
                    result = var
                    break
            if None == result:
                raise OOIObjectError('Requested variable name not found: "%s"' % str(name))

            return result


        def _find_variable_index_by_name(self, name):
            """
            Specialized method for CDM Objects to find the variable object's index by the variable's name
            """
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

            result = -1
            for i in xrange(0, len(self.variables)):
                var = self.variables[i]
                if var is not None and var.name == name:
                    result = i
                    break

            if -1 == result:
                raise OOIObjectError('Requested variable not found: "%s"' % str(name))

            return result


        def _find_attribute_index_by_name(self, name):
            """
            Specialized method for CDM Objects to find the attribute object's index by the attribute's name
            """
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

            result = -1
            for i in xrange(0, len(self.attributes)):
                atrib = self.attributes[i]
                if atrib is not None and atrib.name == name:
                    result = i
                    break

            if -1 == result:
                raise OOIObjectError('Requested attribute not found: "%s"' % str(name))

            return result


        def _cdm_resource_has_attribute(self, name):
            """
            Specialized method for CDM Objects to check existance of an attribute object by its name
            """
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')

            result = False
            for att in self.attributes:
                if att.name == name:
                    result = True
                    break

            return result


        def _remove_attribute(self, name):
            """
            Removes an attribute with the given name from this CDM Object (GROUP)
            @return: The attribute which was removed as a convenience
            """
            idx = _find_attribute_index_by_name(self, name)
            atr = self.attributes.__getitem__(idx)
            self.attributes.__delitem__(idx)

            return atr


        def _set_attribute(self, name, values, data_type=None):
            """
            Specialized method for CDM Objects to set values for existing attributes
            @raise ValueError: When setting an attribute to a different data_type, if
                               data_type is not explicitly specified a ValueError will be raised
            """
            # @attention: Should we allow an empty list of values for an attribute?
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')
            if values is None or (isinstance(values, list) and 0 == len(values)):
                raise ValueError('Invalid argument "values", please specify a non-empty list')
            
            if not isinstance(values, list):
                values = [values]

            atr = _remove_attribute(self, name)
            data_type = data_type or atr.data_type
            try:
                _add_attribute(self, name, int(data_type), values)
                log.warn('Old references to the attribute "%s" are now detached and will not point to the new attribute value' % name)
            except Exception, ex:
                log.warn('WARNING! Exception may have left this resource in an invalid state')
                atr_link = self.attributes.add()
                atr_link.SetLink(atr)
                raise ex


        def _set_dimension(self, name, length):
            """
            Specialized method for CDM Objects to set fields for existing dimensions
            """
            if not isinstance(name, str):
                raise TypeError('Type mismatch for argument "name" -- Expected %s; received %s with value: "%s"' % (repr(str), type(name), str(name)))
            if not name:
                raise ValueError('Invalid argument "name" -- Please specify a non-empty string')
            if not isinstance(length, int):
                raise TypeError('Type mismatch for argument "length" -- Expected %s; received %s with value: "%s"' % (repr(int), type(length), str(length)))
            # @attention: Can a dimension have a length of zero??
            if length <= 0:
                raise ValueError('Invalid argument "dimension": "%s" -- Please specify a positive integer' % str(length))

            dim = _find_dimension_by_name(self, name)

            ### Variable Length is a NETCDF 4 feature which describes a ragged array.
            #if dim.variable_length and length != dim.length:
            #    raise OOIObjectError('Cannot change the length of a dimension when dimension.variable_length is set to False.  Old length: %s.  New length: %s.' % (str(dim.length), str(length)))

            dim.length = length


        #---------------------------------------#
        # Wrapper_Attribute Specialized Methods #
        #---------------------------------------#
        def _get_attribute_value_by_index(self, index = 0):
            """
            Specialized method for CDM Objects to find an attribute value by its index
            """
            if not isinstance(index, int):
                raise OOIObjectError('Invalid array index requested: "%s"' % str(index))
            # @todo: determine if you can have an empty array -- if so, check for empty here
            if index < 0 or index >= len(self.array.value):
                raise OOIObjectError('Given array index out of bounds: %i -- valid range: 0 to %i' % (int(index), len(self.array.value) - 1))

            return self.array.value[index]


        def _get_attribute_values(self):
            """
            Specialized method for CDM Objects to retreive all attribute values as a string list
            """
            # Create a copy of the values array
            result = [item for item in self.array.value]
            return result


        def _get_attribute_values_length(self):
            """
            Specialized method for CDM Objects to find the length of an attribute object's values
            """
            return len(self.array.value)

        def _get_attribute_data_type(self):
            """
            Specialized method for CDM Objects to retrieve the attribute data_type as a long.
            This value can be used to compare equality with other attributes' data_types
            """
            return self.data_type
        
        def _attribute_is_same_type(self, attribute):
            if not hasattr(attribute, 'GetDataType'):
                raise TypeError('The datatype of the given attribute cannot be found.:  Please specify an instance of "%s".  Recieved "%s"' % (type(self), type(attribute)))
            
            return self.GetDataType() == attribute.GetDataType()


        #--------------------------------------#
        # Wrapper_Variable Specialized Methods #
        #--------------------------------------#
        def _get_var_units(self):
            """
            Specialized method for CDM Objects to retrieve the value of a variable object's 'units' attribute
            """
            result = None
            units = _find_attribute_by_name(self, 'units')
            if units != None and len(units.array.value) > 0:
                result = units.array.value[0]

            # @attention: Sometimes (string) attribute values come back as unicode values..  we can provide
            #             a trap here to convert them, but this may not be necessary.  Lets discuss [TPL]
            return result


        def _get_var_std_name(self):
            """
            Specialized method for CDM Objects to retrieve the value of a variable object's 'standard_name' attribute
            """
            result = None
            name = _find_attribute_by_name(self, 'standard_name')
            if name != None and len(name.array.value) > 0:
                result = name.array.value[0]

            # @attention: Sometimes (string) attribute values come back as unicode values..  we can provide
            #             a trap here to convert them, but this may not be necessary.  Lets discuss [TPL]
            return result


        #------------------------------------------------------------------------#
        # Additional helper methods for attaching specialized attributes/methods #
        #------------------------------------------------------------------------#
        def __add_data_type_enum(clsDict):
                # Get a handle to the data_type field from the variable class
                VAR_CLASS = get_gpb_class_from_type_id(CDM_VARIABLE_TYPE)
                field_desc = None
                for name, desc in VAR_CLASS.DESCRIPTOR.fields_by_name.items():
                    if name == "data_type":
                        field_desc = desc

                # Add the enum definitions from the data_type field (can only be one - DataType)
                enum_desc = field_desc.enum_type
                if enum_desc and not enum_desc.name in clsDict:
                    clsDict[enum_desc.name] = EnumObject(enum_desc)


        #--------------------------------------------------------------#
        # Attach specialized methods to object class dictionaries here #
        #--------------------------------------------------------------#
        if obj_type == LINK_TYPE:
            def obj_setlink(self,value):
                if self._invalid:
                    raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

                self.Repository.set_linked_object(self,value)
                if not self.Modified:
                    self._set_parents_modified()
                return

            clsDict['SetLink'] = obj_setlink

        elif obj_type == CDM_DATASET_TYPE:

            clsDict['MakeRootGroup'] = _make_root_group

        elif obj_type == CDM_GROUP_TYPE:

            clsDict['AddGroup'] = _add_group_to_group
            clsDict['AddAttribute'] = _add_attribute
            clsDict['AddDimension'] = _add_dimension
            clsDict['AddVariable'] = _add_variable
            clsDict['FindGroupByName'] = _find_group_by_name
            clsDict['FindAttributeByName'] = _find_attribute_by_name
            clsDict['FindDimensionByName'] = _find_dimension_by_name
            clsDict['FindVariableByName'] = _find_variable_by_name
            clsDict['FindVariableIndexByName'] = _find_variable_index_by_name
            clsDict['FindAttributeIndexByName'] = _find_attribute_index_by_name
            clsDict['HasAttribute'] = _cdm_resource_has_attribute
            clsDict['RemoveAttribute'] = _remove_attribute
            clsDict['SetAttribute'] = _set_attribute
            clsDict['SetDimension'] = _set_dimension

            # Allow the DataType enum to be accessible by this Wrapper...
            __add_data_type_enum(clsDict)


        elif obj_type == CDM_ATTRIBUTE_TYPE:

            clsDict['GetValue'] = _get_attribute_value_by_index
            clsDict['GetValues'] = _get_attribute_values
            # clsDict['SetValue'] = _get_attribute_values
            # clsDict['SetValues'] = _get_attribute_values
            clsDict['GetLength'] = _get_attribute_values_length
            clsDict['GetDataType'] = _get_attribute_data_type
            clsDict['IsSameType'] = _attribute_is_same_type


        elif obj_type == CDM_VARIABLE_TYPE:

            clsDict['GetUnits'] = _get_var_units
            clsDict['GetStandardName'] = _get_var_std_name
            clsDict['AddAttribute'] = _add_attribute
            clsDict['FindAttributeByName'] = _find_attribute_by_name
            clsDict['FindDimensionByName'] = _find_dimension_by_name
            clsDict['FindAttributeIndexByName'] = _find_attribute_index_by_name
            clsDict['HasAttribute'] = _cdm_resource_has_attribute
            clsDict['RemoveAttribute'] = _remove_attribute
            clsDict['SetAttribute'] = _set_attribute
            clsDict['SetDimension'] = _set_dimension

            # @attention: Value adds are currently manual


class Wrapper(object):
    '''
    A Wrapper class for intercepting access to protocol buffers message fields.
    For instance, in the example below I can create a wrapper which is
    read-only.

    To make the wrapper general - apply to more than one kind of protobuffer -
    we can not use descriptors (properties) to transparently intercept a get or
    set request because they are class attributes - shared between all instances
    of the wrapper class. If we add properties each time we create a wrapper
    instance for a new kind of protobuf, new properties will be added to all
    wrapper instances.

    The solution I can up with is clunky! Override the __getattribute__ and
    _setattr__ method to preemptively check a list of fields to get from the
    protocol buffer message. If the key is in the list get/set the deligated
    protocol buffer rather than the wrapper class. The problem is that now we
    can not use the default get/set to initialize our own class or get the list
    of fields!

    Organization:
    The meat of the class is at the top - Init and class methods are at the top
    along with overrides for __getattribute__ and __setattr__ which are the heart
    of the wrapper.

    Below that are all of the methods of protobuffers exposed by the wrapper.

    TODO:
    What about name conflicts between wrapper methods and GPB Fields?


    '''

    __metaclass__ = WrapperType



    def __init__(self, gpbMessage):
        """
        Initialize the Wrapper class and set up it message type.

        """

        self._gpbMessage = gpbMessage
        """
        The gpbMessage is the data object which this instance of wrapper provides
        proxy access to. This wrapper controls both the message object and its
        nested child objects in the case of a composite message. Each child in
        the nested object is returned as a wrapped object all of which are subordinate
        to the wrapper for the root of the composite message.
        """


        self._root=None
        """
        A reference to the root object wrapper for this protobuffer
        A composit protobuffer object may return
        """

        self._invalid=None
        """
        Used to determine whether the wrapper is a currently valid object in the
        version framework. Invalid states can be created when old references to
        an object remain after a differnt version is checked out - similar to
        having a file or directory open that does not exist in the currently
        checked out branch of a source repository.
        """

        # This filed now comes from the metaclass
        #self._gpb_type = None
        """
        The Type ID object for the wrapped object of this class
        """

        self._bytes = None
        """
        Used in _load_element to create a proxy object. The bytes are not loaded
        parsed until the object is needed!
        """

        self._parent_links=None
        """
        A list of all the other wrapper objects which link to me
        """

        self._child_links=None
        """
        A list of my child link wrappers
        """

        self._derived_wrappers=None
        """
        A container for all the wrapper objects which are rewrapped, derived
        from a root object wrapper
        """

        self._myid = None # only exists in the root object
        """
        The name for this object - the SHA1 if it is already hashed or the object
        counter value if it is still in the workspace.
        """

        self._modified =  None # only exists in the root object
        """
        Is this wrapper object modified or commited
        """

        self._read_only = None # only exists in the root object
        """
        Set this to be a read only wrapper!
        Used for commit objects and a history checkout... it is only set in the root object
        """

        self._repository = None # only exists in the root object
        """
        Need to carry a reference to the repository I am in.
        """

        # Hack to prevent setting properties in a class instance
        self._init = True

        #frame = sys._getframe(2)
        #frames = []
        #for i in range(6):
        #    frames.append(frame)
        #    frame = frame.f_back
        #print 'Name: %s, Caller names: ' % type(self), [frame.f_code.co_name for frame in frames]

    @classmethod
    def _create_object(cls, msgtype):
        """
        This is a convience method to create unattached wrapper objects.
        Generally this method should not be used - it is a hook for testing.
        """

        gpbMessage = get_gpb_class_from_type_id(msgtype)()

        obj = cls(gpbMessage)
        obj._repository = None
        obj._root = obj
        obj._parent_links = set()
        obj._child_links = set()
        obj._derived_wrappers={}
        obj._read_only = False
        obj._myid = '-1'
        obj._modified = True
        obj._invalid = False

        return obj

    @property
    def Invalid(self):
        return self._invalid

    def Invalidate(self):
        if self.IsRoot:
            for item in self.DerivedWrappers.values():
                item.Invalidate()

        self._derived_wrappers = None
        self._gpbMessage = None
        self._parent_links = None
        self._child_links = None
        self._myid = None
        self._repository = None
        self._bytes = None
        self._root = None

        self._invalid = True

    @property
    def ObjectClass(self):
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        return self._GPBClass

    @property
    def DESCRIPTOR(self):
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        return self._gpbMessage.DESCRIPTOR

    @property
    def Root(self):
        """
        Access to the root object of the nested GPB object structure
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self._root

    @property
    def IsRoot(self):
        """
        Is this wrapped object the root of a GPB Message?
        GPBs are also tree structures and each element must be wrapped
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self is self._root

    @property
    def ObjectType(self):
        """
        Could just replace the attribute with the capital name?
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        return self._gpb_type

    @property
    def GPBMessage(self):
        """
        Could just replace the attribute with the capital name?
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        # If this is a proxy object which references its serialized value load it!

        bytes = self._bytes
        if  bytes != None:
            self.ParseFromString(bytes)
            self._bytes = None
        return self._gpbMessage

    @property
    def Repository(self):
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        root = self.Root
        return root._repository

    @property
    def DerivedWrappers(self):
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self.Root._derived_wrappers

    def _get_myid(self):
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self.Root._myid

    def _set_myid(self,value):
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        assert isinstance(value, str), 'myid is a string property'
        self.Root._myid = value

    MyId = property(_get_myid, _set_myid)


    def _get_parent_links(self):
        """
        A list of all the wrappers which link to me
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self.Root._parent_links

    def _set_parent_links(self,value):
        """
        A list of all the wrappers which link to me
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        self.Root._parent_links = value

    ParentLinks = property(_get_parent_links, _set_parent_links)

    def _get_child_links(self):
        """
        A list of all the wrappers which I link to
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self.Root._child_links

    def _set_child_links(self, value):
        """
        A list of all the wrappers which I link to
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        self.Root._child_links = value

    ChildLinks = property(_get_child_links, _set_child_links)

    def _get_readonly(self):
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self.Root._read_only

    def _set_readonly(self,value):
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        assert isinstance(value, bool), 'readonly is a boolen property'
        self.Root._read_only = value

    ReadOnly = property(_get_readonly, _set_readonly)

    def _get_modified(self):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self.Root._modified

    def _set_modified(self,value):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        assert isinstance(value, bool), 'modified is a boolen property'
        self.Root._modified = value

    Modified = property(_get_modified, _set_modified)



    def SetLinkByName(self,linkname,value):
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        link = self.GetLink(linkname)
        link.SetLink(value)

    def GetLink(self,linkname):
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')


        gpb = self.GPBMessage
        link = getattr(gpb,linkname)
        link = self._rewrap(link)

        if not link.ObjectType == LINK_TYPE:
            raise OOIObjectError('The field "%s" is not a link!' % linkname)
        return link

    def InParents(self,value):
        '''
        Check recursively to make sure the object is not already its own parent!
        '''
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        for item in self.ParentLinks:
            if item.Root is value:
                return True
            if item.InParents(value):
                return True
        return False

    def SetStructureReadOnly(self):
        """
        Set these objects to be read only
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        self.ReadOnly = True
        for link in self.ChildLinks:
            child = self.Repository.get_linked_object(link)
            child.SetStructureReadOnly()

    def SetStructureReadWrite(self):
        """
        Set these object to be read write!
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        self.ReadOnly = False
        for link in self.ChildLinks:
            child = self.Repository.get_linked_object(link)
            child.SetStructureReadWrite()

    def RecurseCommit(self,structure):
        """
        Recursively build up the serialized structure elements which are needed
        to commit this wrapper and reset all the links using its CAS name.
        """

        #print self.Debug()

        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        self.recurse_count.count += 1
        log.debug('Entering Recurse Commit: recurse counter - %d, child links - %d, objects to commit - %d' % (self.recurse_count.count, len(self.ChildLinks), len(structure)) )

        if not  self.Modified:
            # This object is already committed!
            return

        # Create the Structure Element in which the binary blob will be stored
        se = StructureElement()
        repo = self.Repository

        for link in  self.ChildLinks:

            # Test to see if it is already serialized!
            child_se = repo.index_hash.get(link.key, structure.get(link.key, None))
            #child_se = repo.index_hash.get(link.key, None)

            #print 'Setting child Link:', child_se
            if  child_se is not None:

                # Set the links is leaf property
                link.isleaf = child_se.isleaf

            else:

                #print 'SE for child not found - determining number of child links'

                child = repo.get_linked_object(link)

                # Determine whether this is a leaf node
                if len(child.ChildLinks)==0:
                    link.isleaf = True
                else:
                    link.isleaf = False


                #print 'Calling Recurse Commit on child'
                child.RecurseCommit(structure)

            # Save the link info as a convience for sending!
            se.ChildLinks.add(link.key)

        se.value = self.SerializeToString()
        #se.key = sha1hex(se.value)

        # Structure element wrapper provides for setting type!
        se.type = self.ObjectType

        # Calculate the sha1 from the serialized value and type!
        # Sha1 is a property - not a method...
        se.key = se.sha1

        # Determine whether I am a leaf
        if len(self.ChildLinks)==0:
            se.isleaf=True
        else:
            se.isleaf = False

        # Done setting up the Sturcture Element
        structure[se.key] = se

        # This will be true for any object which is not a core object such as a commit
        # We don't want to worry about what is in the workspace - that is the repositories job.
        # if I am currently in the work space the commited version of me should be too!
        if repo._workspace.has_key(self.MyId):
            # Remove my old name
            del repo._workspace[self.MyId]

            # Now deal with some nastyness
            # Possible DAG structure created by hash conflict - two wrappers of the same type with the same value in one data structure
            if se.key in repo._workspace:

                # Get the other object with the same name...
                other = repo._workspace[se.key]


                # if the value of a field has been set to the same value again,
                # it will be re serialized and re hashed. This is not a conflict!
                if not other is self:


                    self.ParentLinks.update(other.ParentLinks)

                    # Here, we don't want to modify a parent if it is already correct.
                    # The hash conflict provides a back door by which a parent, which is
                    # already benn correctly committed might be modified if we are not careful
                    for link in self.ParentLinks:


                        # if it is invalid - the parent has already been serialized
                        if not link.Invalid and link.key != se.key:
                            link.key = se.key
                    """
                    msg = '=============================================\n'
                    msg += '''DAG structure created by hash conflict - two wrappers of the same type with the same value in one data structure. This is not an error, but the state of this composite is now shared.\n'''
                    msg +='Shared Object: %s' % self.Debug()
                    msg += 'Shared Parents:\n'
                    for link in self.ParentLinks:
                        msg +='Parent: %s' % (link.Root.Debug())
                    msg +='Old references to the object are now invalid!\n'
                    msg += '============================================='
                    log.warn(msg)
                    """
                    
                    # Force the object to be reloaded from the workbench!
                    del repo._workspace[se.key]
                    other.Invalidate()
                    self.Invalidate()

                    # We are done - get outta here!
                    return

            else:
                repo._workspace[se.key] = self


        self.MyId = se.key
        self.Modified = False

        # Set the key value for parent links!
        # This will only be reached once for a given child object. Set all parents
        # now and the child will return as unmodified when the other parents ask it
        # to recurse commit.

        for link in self.ParentLinks:
            link.key = self.MyId








    def FindChildLinks(self):
        """
        Find all of the links in this composit structure
        All of the objects worked on in this method are raw proto buffers messages!
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        gpb = self.GPBMessage
        # For each field in the protobuffer message
        for field in gpb.DESCRIPTOR.fields:
            # if the field is a composite - another message
            if field.message_type:

                # Get the field of type message
                gpb_field = getattr(gpb,field.name)


                # If it is a repeated container type
                if isinstance(gpb_field, containers.RepeatedCompositeFieldContainer):

                    for item in gpb_field:

                        wrapped_item = self._rewrap(item)
                        if wrapped_item.ObjectType == LINK_TYPE:
                            self.ChildLinks.add(wrapped_item)
                        else:
                            wrapped_item.FindChildLinks()

                # IF it is a standard message field
                else:
                    if not gpb_field.IsInitialized():
                        # if it is an optional field which is not initialized
                        # it can not hold any links!
                        continue

                    item = self._rewrap(gpb_field)
                    if item.ObjectType == LINK_TYPE:
                        self.ChildLinks.add(item)
                    else:
                        item.FindChildLinks()


    def AddParentLink(self, link):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        for parent in self.ParentLinks:

            if parent.GPBMessage is link.GPBMessage:
                break
        else:
            self.ParentLinks.add(link)

    def _rewrap(self, gpbMessage):
        '''
        Factory method to return a new instance of wrapper for a gpbMessage
        from self - used for access to composite structures, it has all the same
        shared variables as the parent wrapper
        '''
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        # Check the root wrapper objects list of derived wrappers
        if gpbMessage in self.DerivedWrappers:
            return self.DerivedWrappers[gpbMessage]

        # Else make a new one...
        inst = Wrapper(gpbMessage)
        inst._root = self._root

        # Add it to the list of objects which derive from the root wrapper
        self.DerivedWrappers[gpbMessage] = inst

        return inst

    def _set_parents_modified(self):
        """
        This method recursively changes an objects parents to a modified state
        All links are reset as they are no longer hashed values
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if self.Modified:
            # Be clear about what we are doing here!
            # If it has already been modified we are done.
            return
        else:

            self.Modified = True

            # Get the repository
            repo = self.Repository

            new_id = repo.new_id()
            repo._workspace[new_id] = self.Root

            if repo._workspace.has_key(self.MyId):
                del repo._workspace[self.MyId]
            self.MyId = new_id

            # When you hit the commit ref - stop!
            if self.Root is repo._workspace_root:
                # The commit is no longer really your parent!
                self.ParentLinks = set()

            else:

                for link in self.ParentLinks:
                    # Tricky - set the message directly and call modified!
                    #link.GPBMessage.key = self.MyId
                    link.GPBMessage.key = self.MyId
                    #link._set_parents_modified()
                    link._set_parents_modified()

    def __eq__(self, other):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if not isinstance(other, Wrapper):
            return False

        if self is other:
            return True

        return self.GPBMessage == other.GPBMessage

    def __ne__(self, other):
        # Can't just say self != other_msg, since that would infinitely recurse. :)
        return not self == other

    def __str__(self):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if self.ObjectType == LINK_TYPE:
            key = self.GPBMessage.key
            try:
                key = sha1_to_hex(self.GPBMessage.key)
            except struct.error, er:
                pass
            msg = '\nkey: %s \ntype { %s }' % (key, self.GPBMessage.type)
        else:
            msg = '\n' +self.GPBMessage.__str__()

        return msg

    def Debug(self):
        output  = '================== Wrapper (Modified = %s)====================\n' % self.Modified
        output += 'Wrapper ID: %s \n' % self.MyId
        output += 'Wrapper IsRoot: %s \n' % self.IsRoot
        output += 'Wrapper ParentLinks: %s \n' % str(self.ParentLinks)
        output += 'Wrapper ChildLinks: %s \n' % str(self.ChildLinks)
        output += 'Wrapper Type: %s \n' % str(self.ObjectType)
        output += 'Wrapper current value:\n'
        output += str(self) + '\n'
        output += '================== Wrapper Complete =========================\n'
        return output

    def IsInitialized(self):
        """Checks if the message is initialized.

        Returns:
            The method returns True if the message is initialized (i.e. all of its
        required fields are set).
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        return self.GPBMessage.IsInitialized()

    def SerializeToString(self):
        """Serializes the protocol message to a binary string.

        Returns:
          A binary string representation of the message if all of the required
        fields in the message are set (i.e. the message is initialized).

        Raises:
          message.EncodeError if the message isn't initialized.
        """
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        try:
            serialized = self.GPBMessage.SerializeToString()
        except message.EncodeError, ex:
            log.info(ex)
            raise OOIObjectError('Could not serialize object - likely due to unset required field in a core object: %s' % str(self))

        return serialized

    def ParseFromString(self, serialized):
        """Clear the message and read from serialized."""
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        # Do not use the GPBMessage method - it will recurse!
        self._gpbMessage.ParseFromString(serialized)

    def ListSetFields(self):
        """Returns a list of (FieldDescriptor, value) tuples for all
        fields in the message which are not empty.  A singular field is non-empty
        if IsFieldSet() would return true, and a repeated field is non-empty if
        it contains at least one element.  The fields are ordered by field
        number"""
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        field_list = self.GPBMessage.ListFields()
        fnames=[]
        for desc, val in field_list:
            fnames.append(desc.name)
        return fnames

    def IsFieldSet(self, field_name):
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        GPBMessage = self.GPBMessage
        # Get the raw GPB field
        try:
            GPBField = getattr(GPBMessage, field_name)
        except AttributeError, ex:
            raise OOIObjectError('The "%s" object definition does not have a field named "%s"' % \
                    (str(self.ObjectClass), field_name))

        if isinstance(GPBField, containers.RepeatedScalarFieldContainer):
            return len(GPBField) > 0

        elif isinstance(GPBField, containers.RepeatedCompositeFieldContainer):
            if len(GPBField) == 0:
                return False
            for item in GPBField:
                if len(item.ListFields())>0:
                    return True
            else:
                return False

        else:
            return GPBMessage.HasField(field_name)

    def HasField(self, field_name):
        log.warn('HasField is depricated because the name is confusing. Use IsFieldSet')
        return self.IsFieldSet(field_name)

    def ClearField(self, field_name):
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        GPBMessage = self.GPBMessage

        #if not GPBMessage.IsFieldSet(field_name):
        #    # Nothing to clear
        #    return

        # Get the raw GPB field
        try:
            GPBField = getattr(GPBMessage, field_name)
        except AttributeError, ex:
            raise OOIObjectError('The "%s" object definition does not have a field named "%s"' % \
                    (str(self.ObjectClass), field_name))

        if isinstance(GPBField, containers.RepeatedScalarFieldContainer):
            del self.DerivedWrappers[GPBField]
            # Nothing to do - just clear the field. It can not contain a link

        elif isinstance(GPBField, containers.RepeatedCompositeFieldContainer):
            for item in GPBField:
                wrapped_field = self._rewrap(item)
                wrapped_field._clear_derived_message()

                del self.DerivedWrappers[item]

            del self.DerivedWrappers[GPBField]

        elif isinstance(GPBField, message.Message):
            wrapped_field = self._rewrap(GPBField)
            wrapped_field._clear_derived_message()

            del self.DerivedWrappers[GPBField]

        # Nothing to be done for scalar fields - just clear it.

        #Now clear the field
        self.GPBMessage.ClearField(field_name)
        # Set this object and it parents to be modified
        self._set_parents_modified()

    def _clear_derived_message(self):
        """
        Helper method for ClearField
        """
        if self.ObjectType == LINK_TYPE:
            child_obj = self.Repository.get_linked_object(self)
            # Remove this link from the list of parents
            child_obj.ParentLinks.remove(self)

            # This is the only one, remove it as a child
            self.ChildLinks.remove(self)

        for field_name in self.DESCRIPTOR.fields_by_name.keys():
            # Recursively remove all
            self.ClearField(field_name)

    #def HasExtension(self, extension_handle):
    #    return self.GPBMessage.HasExtension(extension_handle)

    #def ClearExtension(self, extension_handle):
    #    return self.GPBMessage.ClearExtension(extension_handle)

    def ByteSize(self):
        """Returns the serialized size of this message.
        Recursively calls ByteSize() on all contained messages.
        """
        #if self.Invalid:
        if self._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self.GPBMessage.ByteSize()



class ContainerWrapper(object):
    """
    This class is only for use with containers.RepeatedCompositeFieldContainer
    It is not needed for repeated scalars!
    """

    def __init__(self, wrapper, gpbcontainer):
        # Be careful - this is a hard link
        self._wrapper = wrapper
        if not isinstance(gpbcontainer, containers.RepeatedCompositeFieldContainer):
            raise OOIObjectError('The Container Wrapper is only for use with Repeated Composit Field Containers')
        self._gpbcontainer = gpbcontainer
        self.Repository = wrapper.Repository

    @classmethod
    def factory(cls, wrapper, gpbcontainer):

        # Check the root wrapper objects list of derived wrappers before making a new one
        if wrapper._invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        #print cls, type(wrapper), type(gpbcontainer)
        #objhash = hash(gpbcontainer)
        dw = wrapper.DerivedWrappers
        if gpbcontainer in dw:
            return dw[gpbcontainer]

        inst = cls(wrapper, gpbcontainer)

        # Add it to the list of objects which derive from the root wrapper
        dw[gpbcontainer] = inst
        return inst


    @property
    def Root(self):
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        return self._wrapper.Root

    @property
    def Invalid(self):
        if not self._wrapper:
            return True
        return self._wrapper._invalid

    def Invalidate(self):
        self._gpbcontainer = None
        self._wrapper = None
        self.Repository = None

    def __setitem__(self, key, value):
        """Sets the item on the specified position.
        Depricated"""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if not isinstance(value, Wrapper):
            raise OOIObjectError('To set an item in a repeated field container, the value must be a Wrapper')

        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper._rewrap(item)
        if item.ObjectType == LINK_TYPE:
            self.Repository.set_linked_object(item, value)
        else:
            raise OOIObjectError('It is illegal to set a value of a repeated composit field unless it is a CASRef - Link')

        self._wrapper._set_parents_modified()


    def SetLink(self,key,value):

        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if not isinstance(value, Wrapper):
            raise OOIObjectError('To set an item in a repeated field container, the value must be a Wrapper')

        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper._rewrap(item)
        if item.ObjectType == LINK_TYPE:
            self.Repository.set_linked_object(item, value)
        else:
            raise OOIObjectError('It is illegal to set a value of a repeated composit field unless it is a CASRef - Link')

        self._wrapper._set_parents_modified()


    def __getitem__(self, key):
        """Retrieves item by the specified key."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        value = self._gpbcontainer.__getitem__(key)
        value = self._wrapper._rewrap(value)
        if value.ObjectType == LINK_TYPE:
            value = self.Repository.get_linked_object(value)
        return value

    def GetLink(self,key):

        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        link = self._gpbcontainer.__getitem__(key)
        link = self._wrapper._rewrap(link)
        assert link.ObjectType == LINK_TYPE, 'The field "%s" is not a link!' % linkname
        return link

    def GetLinks(self):
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        wrapper_list=[]
        links = self._gpbcontainer[:] # Get all the links!
        for link in links:
            link = self._wrapper._rewrap(link)
            assert link.ObjectType == LINK_TYPE, 'The field "%s" is not a link!' % linkname
            wrapper_list.append(link)
        return wrapper_list

    def __len__(self):
        """Returns the number of elements in the container."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self._gpbcontainer.__len__()

    def __ne__(self, other):
        """Checks if another instance isn't equal to this one."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if not isinstance(other, self.__class__):
            raise OOIObjectError('Can only compare repeated composite fields against other repeated composite fields.')
        # The concrete classes should define __eq__.
        return not self._gpbcontainer == other._gpbcontainer

    def __eq__(self, other):
        """Compares the current instance with another one."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if self is other:
            return True

        if not isinstance(other, self.__class__):
            raise OOIObjectError('Can only compare repeated composite fields against other repeated composite fields.')
        return self._gpbcontainer == other._gpbcontainer

    def __repr__(self):
        """Need to improve this!"""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self._gpbcontainer.__repr__()


    # Composite specific methods:
    def add(self):
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        new_element = self._gpbcontainer.add()

        self._wrapper._set_parents_modified()
        return self._wrapper._rewrap(new_element)

    def __getslice__(self, start, stop):
        """Retrieves the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        wrapper_list=[]
        for index in range(0, len(self))[start:stop]:
            wrapper_list.append(self.__getitem__(index))

        # Does it make sense to return a list?
        return wrapper_list

    def __delitem__(self, key):
        """Deletes the item at the specified position."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        self._wrapper._set_parents_modified()

        item = self._gpbcontainer.__getitem__(key)
        item = self._wrapper._rewrap(item)

        item._clear_derived_message()

        self._gpbcontainer.__delitem__(key)

    def __delslice__(self, start, stop):
        """Deletes the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        i_range = range(0, len(self))[start:stop]
        for index in reversed(i_range):
            self.__delitem__(index)



class ScalarContainerWrapper(object):
    """
    This class is only for use with containers.RepeatedCompositeFieldContainer
    It is not needed for repeated scalars!
    """

    def __init__(self, wrapper, gpbcontainer):
        # Be careful - this is a hard link
        self._wrapper = wrapper
        if not isinstance(gpbcontainer, containers.RepeatedScalarFieldContainer):
            raise OOIObjectError('The Container Wrapper is only for use with Repeated Composit Field Containers')
        self._gpbcontainer = gpbcontainer
        self.Repository = wrapper.Repository

    @classmethod
    def factory(cls, wrapper, gpbcontainer):

        # Check the root wrapper objects list of derived wrappers before making a new one
        #objhash = hash(gpbcontainer)
        dw = wrapper.DerivedWrappers
        if gpbcontainer in dw:
            return dw[gpbcontainer]

        inst = cls(wrapper, gpbcontainer)

        # Add it to the list of objects which derive from the root wrapper
        dw[gpbcontainer] = inst
        return inst

    @property
    def Invalid(self):
        if not self._wrapper:
            return True
        return self._wrapper._invalid

    def Invalidate(self):
        self._gpbcontainer = None
        self._wrapper = None
        self.Repository = None

    def append(self, value):
        """Appends an item to the list. Similar to list.append()."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        self._gpbcontainer.append(value)
        self._wrapper._set_parents_modified()

    def insert(self, key, value):
        """Inserts the item at the specified position. Similar to list.insert()."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        self._gpbcontainer.insert(key, value)
        self._wrapper._set_parents_modified()

    def extend(self, elem_seq):
        """Extends by appending the given sequence. Similar to list.extend()."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        self._gpbcontainer.extend(elem_seq)
        self._wrapper._set_parents_modified()

    def remove(self, elem):
        """Removes an item from the list. Similar to list.remove()."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        self._gpbcontainer.remove(elem)
        self._wrapper._set_parents_modified()


    def __getslice__(self, start, stop):
        """Retrieves the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self._gpbcontainer._values[start:stop]

    def __len__(self):
        """Returns the number of elements in the container."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return len(self._gpbcontainer._values)

    def __getitem__(self, key):
        """Retrieves the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return self._gpbcontainer._values[key]

    def __setitem__(self, key, value):
        """Sets the item on the specified position."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        self._gpbcontainer.__setitem__(key, value)
        self._wrapper._set_parents_modified()

    def __setslice__(self, start, stop, values):
        """Sets the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        self._gpbcontainer.__setslice__(start, stop, values)
        self._wrapper._set_parents_modified()

    def __delitem__(self, key):
        """Deletes the item at the specified position."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        del self._gpbcontainer._values[key]
        self._gpbcontainer._message_listener.Modified()
        self._wrapper._set_parents_modified()

    def __delslice__(self, start, stop):
        """Deletes the subset of items from between the specified indices."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        self._gpbcontainer._values.__delslice__(start,stop)
        self._gpbcontainer._message_listener.Modified()
        self._wrapper._set_parents_modified()

    def __eq__(self, other):
        """Compares the current instance with another one."""
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')

        if self is other:
            return True
        # Special case for the same type which should be common and fast.
        if isinstance(other, self.__class__):
            return other._gpbcontainer._values == self._gpbcontainer._values
        # We are presumably comparing against some other sequence type.
        return other == self._gpbcontainer._values

    def __ne__(self, other):
        """Checks if another instance isn't equal to this one."""
        # The concrete classes should define __eq__.
        return not self == other

    def __repr__(self):
        if self.Invalid:
            raise OOIObjectError('Can not access Invalidated Object which may be left behind after a checkout or reset.')
        return repr(self._gpbcontainer._values)

class StructureElementError(Exception):
    """
    An error class for structure elements
    """


class StructureElement(object):
    """
    @brief Wrapper for the container structure element. These are the objects
    stored in the hashed elements table. Mostly convience methods are provided
    here. A set provides references to the child objects so that the content
    need not be decoded to find them.
    """
    def __init__(self,se=None):

        if se:
            self._element = se
        else:
            self._element = get_gpb_class_from_type_id(STRUCTURE_ELEMENT_TYPE)()
        self.ChildLinks = set()

    @classmethod
    def parse_structure_element(cls,blob):

        se = get_gpb_class_from_type_id(STRUCTURE_ELEMENT_TYPE)()
        se.ParseFromString(blob)

        instance = cls(se)

        if instance.key != instance.sha1:
            log.error('The sha1 key does not match the value. The data is corrupted! \n' +\
            'Element key %s, Calculated key %s' % (sha1_to_hex(instance.key), sha1_to_hex(instance.sha1)))
            raise StructureElementError('Error reading serialized structure element. Sha1 value does not match.')

        return instance

    @property
    def sha1(self):
        """
        Make the sha1 safe for empty contents but also type safe.
        Take use the sha twice so that we don't need to concatinate long strings!
        """
        #################
        ## This is the method that you can compare in Java
        #################
        ## Get the length of the binary arrays
        #sha_len = 20
        #type_len = self.type.ByteSize()
        #
        ## Convert to signed integer bytes
        #fmt = '!%db' % type_len
        #type_bytes = struct.unpack('!%db' % type_len , self.type.SerializeToString())
        #
        ## Convert the sha1 of the content to signed integer bytes
        #c_sha_bytes = struct.unpack('!20b', sha1bin(self.value))
        #
        ## Concatinate the the byte arrays as integers
        #cat_bytes = list(c_sha_bytes) + list(type_bytes)
        #
        ## Get the length of the concatination and convert to byte array
        #fmt = '!%db' % (type_len+sha_len)
        #sha_cat = struct.pack(fmt, *cat_bytes)
        #
        ##print 'sha1hex(sha_cat):',sha1hex(sha_cat)
        ##print 'sha1hex(sha1bin(self.value) + self.type.SerializeToString()):',sha1hex(sha1bin(self.value) + self.type.SerializeToString())
        #
        ## Return the sha1 of the byte array
        #return sha1bin(sha_cat)
        #################
        # This does the same thing much faster and shorter!
        #################
        return sha1bin(sha1bin(self.value) + self.type.SerializeToString())

    #@property
    def _get_type(self):
        return self._element.type

    #@type.setter
    def _set_type(self,obj_type):
        self._element.type.object_id = obj_type.object_id
        self._element.type.version = obj_type.version

    type = property(_get_type, _set_type)

    #@property
    def _get_value(self):
        return self._element.value

    #@value.setter
    def _set_value(self,value):
        self._element.value = value

    value = property(_get_value, _set_value)

    #@property
    def _get_key(self):
        #return sha1_to_hex(self._element.key)
        return self._element.key

    #@key.setter
    def _set_key(self,value):
        self._element.key = value

    key = property(_get_key, _set_key)

    def _set_isleaf(self,value):
        self._element.isleaf = value

    def _get_isleaf(self):
        return self._element.isleaf

    isleaf = property(_get_isleaf, _set_isleaf)

    def __str__(self):
        msg = ''
        if len(self._element.key)==20:
            msg  = 'Hexkey: "'+sha1_to_hex(self._element.key) +'"\n'
        return msg + self._element.__str__()

    def serialize(self):
        return self._element.SerializeToString()

