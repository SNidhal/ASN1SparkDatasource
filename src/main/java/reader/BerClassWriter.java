/*
 * Copyright 2011-17 Fraunhofer ISE
 *
 * This file is part of jASN1.
 * For more information visit http://www.openmuc.org
 *
 * jASN1 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 2.1 of the License, or
 * (at your option) any later version.
 *
 * jASN1 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with jASN1.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package reader;

import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.openmuc.jasn1.compiler.model.*;
import org.openmuc.jasn1.compiler.model.AsnModule.TagDefault;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.spark.sql.types.DataTypes.*;

public class BerClassWriter {
    public int i=0;
    private static Tag stdSeqTag = new Tag();
    private static Tag stdSetTag = new Tag();

    static {
        stdSeqTag.tagClass = TagClass.UNIVERSAL;
        stdSeqTag.value = 16;
        stdSeqTag.typeStructure = TypeStructure.CONSTRUCTED;

        stdSetTag.tagClass = TagClass.UNIVERSAL;
        stdSetTag.value = 17;
        stdSetTag.typeStructure = TypeStructure.CONSTRUCTED;
    }

    public enum TagClass {
        UNIVERSAL,
        APPLICATION,
        CONTEXT,
        PRIVATE
    };

    public enum TagType {
        EXPLICIT,
        IMPLICIT
    };

    public enum TypeStructure {
        PRIMITIVE,
        CONSTRUCTED
    }

    public static class Tag {
        public int value;
        public TagClass tagClass;
        public TagType type;
        public TypeStructure typeStructure;
    }

    private static final Set<String> reservedKeywords = Collections
            .unmodifiableSet(new TreeSet<>(Arrays.asList("public", "private", "protected", "final", "void", "int",
                    "short", "float", "double", "long", "byte", "char", "String", "throw", "throws", "new", "static",
                    "volatile", "if", "else", "for", "switch", "case", "enum", "this", "super", "boolean", "class",
                    "abstract", "package", "import", "null", "code", "getClass", "setClass")));

    private TagDefault tagDefault;

    private final HashMap<String, AsnModule> modulesByName;
    private AsnModule module;

    BerClassWriter(HashMap<String, AsnModule> modulesByName) {
        this.modulesByName = modulesByName;
    }

    public void translate() throws IOException {
        for (AsnModule module : modulesByName.values()) {
            for (SymbolsFromModule symbolsFromModule : module.importSymbolFromModuleList) {
                if (modulesByName.get(symbolsFromModule.modref) == null) {
                    throw new IOException("Module \"" + module.moduleIdentifier.name + "\" imports missing module \""
                            + symbolsFromModule.modref + "\".");
                }
            }
        }

        for (AsnModule module : modulesByName.values()) {
            translateModule(module);
        }

    }

    public void translateModule(AsnModule module) throws IOException {
        this.module = module;
        tagDefault = module.tagDefault;

        for (AsnType typeDefinition : module.typesByName.values()) {
            if (typeDefinition instanceof AsnDefinedType) {
                if (getInformationObjectClass(((AsnDefinedType) typeDefinition).typeName, module) != null) {
                    continue;
                }
            }

            String typeName = cleanUpName(typeDefinition.name);


            if (typeDefinition instanceof AsnTaggedType) {

                AsnTaggedType asnTaggedType = (AsnTaggedType) typeDefinition;

                Tag tag = getTag(asnTaggedType);

                if (asnTaggedType.definedType != null) {
                }
                else {

                    AsnType assignedAsnType = asnTaggedType.typeReference;

                    if (assignedAsnType instanceof AsnConstructedType) {
                        writeConstructedTypeClass(typeName, assignedAsnType, tag, false, null,false);
                    }
                    else {
                    }
                }

            }
            else if (typeDefinition instanceof AsnConstructedType) {
                writeConstructedTypeClass(typeName, typeDefinition, null, false, null,false);
            }
            else {
            }

        }


    }



    private String sanitizeModuleName(String name) {
        String[] moduleParts = name.split("-");
        String toReturn = "";
        for (String part : moduleParts) {
            toReturn += sanitize(part.toLowerCase()) + "-";
        }
        if (!toReturn.isEmpty()) {
            toReturn = toReturn.substring(0, toReturn.length() - 1);
        }
        return toReturn;
    }




    /**
     * Gets the tag from the AsnTaggedType structure. The returned tag will contain the correct class and type (explicit
     * or implicit). Return null if the passed tagged type does not have a tag.
     *
     * @param asnTaggedType
     * @return the tag from the AsnTaggedType structure
     * @throws IOException
     */
    private Tag getTag(AsnTaggedType asnTaggedType) throws IOException {

        AsnTag asnTag = asnTaggedType.tag;

        if (asnTag == null) {
            return null;
        }

        Tag tag = new Tag();

        String tagClassString = asnTag.clazz;
        if (tagClassString.isEmpty() || "CONTEXT".equals(tagClassString)) {
            tag.tagClass = TagClass.CONTEXT;
        }
        else if ("APPLICATION".equals(tagClassString)) {
            tag.tagClass = TagClass.APPLICATION;
        }
        else if ("PRIVATE".equals(tagClassString)) {
            tag.tagClass = TagClass.PRIVATE;
        }
        else if ("UNIVERSAL".equals(tagClassString)) {
            tag.tagClass = TagClass.UNIVERSAL;
        }
        else {
            throw new IllegalStateException("unknown tag class: " + tagClassString);
        }

        String tagTypeString = asnTaggedType.tagType;

        if (tagTypeString.isEmpty()) {
            if (tagDefault == TagDefault.EXPLICIT) {
                tag.type = TagType.EXPLICIT;
            }
            else {
                tag.type = TagType.IMPLICIT;
            }
        }
        else if (tagTypeString.equals("IMPLICIT")) {
            tag.type = TagType.IMPLICIT;
        }
        else if (tagTypeString.equals("EXPLICIT")) {
            tag.type = TagType.EXPLICIT;
        }
        else {
            throw new IllegalStateException("unexpected tag type: " + tagTypeString);
        }

        if (tag.type == TagType.IMPLICIT) {
            if (isDirectAnyOrChoice(asnTaggedType)) {
                tag.type = TagType.EXPLICIT;
            }
        }

        if ((tag.type == TagType.IMPLICIT) && isPrimitive(asnTaggedType)) {
            tag.typeStructure = TypeStructure.PRIMITIVE;
        }
        else {
            tag.typeStructure = TypeStructure.CONSTRUCTED;
        }

        tag.value = asnTaggedType.tag.classNumber.num;

        return tag;

    }

    private String cleanUpName(String name) {

        name = replaceCharByCamelCase(name, '-');
        name = replaceCharByCamelCase(name, '_');

        return sanitize(name);
    }

    private String sanitize(String name) {
        String result = replaceCharByCamelCase(name, '.');
        if (reservedKeywords.contains(result)) {
            result += "_";
        }
        return result;
    }

    private String replaceCharByCamelCase(String name, char charToBeReplaced) {
        StringBuilder nameSb = new StringBuilder(name);

        int index = name.indexOf(charToBeReplaced);
        while (index != -1 && index != (name.length() - 1)) {
            if (!Character.isUpperCase(name.charAt(index + 1))) {
                nameSb.setCharAt(index + 1, Character.toUpperCase(name.charAt(index + 1)));
            }
            index = name.indexOf(charToBeReplaced, index + 1);
        }

        name = nameSb.toString();
        name = name.replace("" + charToBeReplaced, "");

        return name;
    }

    private void writeConstructedTypeClass(String className, AsnType asnType, Tag tag, boolean asInternalClass,
                                           List<String> listOfSubClassNames,boolean nested) throws IOException {

        if (listOfSubClassNames == null) {
            listOfSubClassNames = new ArrayList<>();
        }

        String isStaticStr = "";
        if (asInternalClass) {
            isStaticStr = " static";
        }

        if (asnType instanceof AsnSequenceSet) {
            writeSequenceOrSetClass(className, (AsnSequenceSet) asnType, tag, isStaticStr, listOfSubClassNames,nested);
        }
        else if (asnType instanceof AsnSequenceOf) {
            writeSequenceOfClass((AsnSequenceOf) asnType, listOfSubClassNames);
        }
        else if (asnType instanceof AsnChoice) {
            writeChoiceClass(className, (AsnChoice) asnType, tag, isStaticStr, listOfSubClassNames);
        }
    }

    private void writeChoiceClass(String className, AsnChoice asn1TypeElement, Tag tag, String isStaticStr,
                                  List<String> listOfSubClassNames) throws IOException {


        List<AsnElementType> componentTypes = asn1TypeElement.componentTypes;

        addAutomaticTagsIfNeeded(componentTypes);

        if (asn1TypeElement.parameters != null) {
            List<AsnParameter> parameters = asn1TypeElement.parameters;
            replaceParamtersByAnyTypes(componentTypes, parameters);
        }

        for (AsnElementType componentType : componentTypes) {
            if (componentType.typeReference != null && (componentType.typeReference instanceof AsnConstructedType)) {
                listOfSubClassNames.add(getClassNameOfComponent(componentType));
            }
        }

        for (AsnElementType componentType : componentTypes) {

            if (isInnerType(componentType)) {

                String subClassName = getClassNameOfComponent(componentType);
                writeConstructedTypeClass(subClassName, componentType.typeReference, null, true, listOfSubClassNames,true);

            }
        }

        setClassNamesOfComponents(listOfSubClassNames, componentTypes);

        writePublicMembers(componentTypes,false);
    }


    private void setClassNamesOfComponents(List<String> listOfSubClassNames, List<AsnElementType> componentTypes) {
        for (AsnElementType element : componentTypes) {
            element.className = getClassNameOfComponent(listOfSubClassNames, element);
        }
    }

    private String getClassNameOfComponent(AsnElementType asnElementType) throws IOException {
        if (asnElementType.className != "") {
            return asnElementType.className;
        }
        return getClassNameOfComponent(null, asnElementType);
    }

    private String getClassNameOfComponent(List<String> listOfSubClassNames, AsnTaggedType element) {

        if (listOfSubClassNames == null) {
            listOfSubClassNames = new ArrayList<>();
        }

        if (element.typeReference == null) {

            if (element.definedType.isObjectClassField) {

                AsnInformationObjectClass informationObjectClass = getInformationObjectClass(
                        element.definedType.moduleOrObjectClassReference, module);
                if (informationObjectClass == null) {
                    throw new CompileException("no information object class of name \""
                            + element.definedType.moduleOrObjectClassReference + "\" found");
                }

                for (AsnElementType elementType : informationObjectClass.elementList) {
                    if (elementType.name.equals(element.definedType.typeName)) {
                        return getClassNameOfComponent(listOfSubClassNames, elementType);
                    }
                }

                throw new IllegalStateException(
                        "Could not find field \"" + element.definedType.typeName + "\" of information object class \""
                                + element.definedType.moduleOrObjectClassReference + "\"");
            }
            else {

                String cleanedUpClassName = cleanUpName(element.definedType.typeName);
                for (String subClassName : listOfSubClassNames) {
                    if (subClassName.equals(cleanedUpClassName)) {
                        String moduleName = module.moduleIdentifier.name;

                        for (SymbolsFromModule syms : this.module.importSymbolFromModuleList) {
                            if (syms.symbolList.contains(element.definedType.typeName)) {
                                moduleName = syms.modref;
                                break;
                            }
                        }

                        return   sanitizeModuleName(moduleName).replace('-', '.').toLowerCase() + "."
                                + cleanedUpClassName;

                    }

                }
                return cleanedUpClassName;
            }
        }
        else {
            AsnType typeDefinition = element.typeReference;

            if (typeDefinition instanceof AsnConstructedType) {
                return cleanUpName(capitalizeFirstCharacter(element.name));
            }
            else {
                return getBerType(typeDefinition);
            }

        }

    }

    private AsnInformationObjectClass getInformationObjectClass(String objectClassReference, AsnModule module) {
        AsnInformationObjectClass ioClass = module.objectClassesByName.get(objectClassReference);
        if (ioClass == null) {
            AsnType asnType = module.typesByName.get(objectClassReference);
            if (asnType == null) {

                return null;
            }
            else {
                if (asnType instanceof AsnDefinedType) {
                    return getInformationObjectClass(((AsnDefinedType) asnType).typeName, module);
                }
            }
        }
        return ioClass;
    }

    private void writeSequenceOrSetClass(String className, AsnSequenceSet asnSequenceSet, Tag tag, String isStaticStr,
                                         List<String> listOfSubClassNames,boolean nested) throws IOException {

        List<AsnElementType> componentTypes = asnSequenceSet.componentTypes;

        addAutomaticTagsIfNeeded(componentTypes);

        if (asnSequenceSet.parameters != null) {
            List<AsnParameter> parameters = asnSequenceSet.parameters;
            replaceParamtersByAnyTypes(componentTypes, parameters);
        }

        for (AsnElementType componentType : componentTypes) {
            if (componentType.typeReference != null && (componentType.typeReference instanceof AsnConstructedType)) {
                listOfSubClassNames.add(getClassNameOfComponent(componentType));
            }
        }

        for (AsnElementType componentType : componentTypes) {

            if (isInnerType(componentType)) {

                String subClassName = getClassNameOfComponent(componentType);

                writeConstructedTypeClass(subClassName, componentType.typeReference, null, true, listOfSubClassNames,true);

            }

        }


        setClassNamesOfComponents(listOfSubClassNames, componentTypes);

        writePublicMembers(componentTypes,nested);


    }

    private void replaceParamtersByAnyTypes(List<AsnElementType> componentTypes, List<AsnParameter> parameters) {
        for (AsnParameter parameter : parameters) {
            if (parameter.paramGovernor == null) {
                for (AsnElementType componentType : componentTypes) {
                    if (componentType.definedType != null
                            && componentType.definedType.typeName.equals(parameter.dummyReference)) {

                        componentType.typeReference = new AsnAny();
                        componentType.definedType = null;
                        componentType.isDefinedType = false;
                    }
                }
            }
        }
    }




    private void writeSequenceOfClass( AsnSequenceOf asnSequenceOf,
                                      List<String> listOfSubClassNames) throws IOException {


        AsnElementType componentType = asnSequenceOf.componentType;

        String referencedTypeName = getClassNameOfSequenceOfElement(componentType, listOfSubClassNames);

        if (isInnerType(componentType)) {
            writeConstructedTypeClass(referencedTypeName, componentType.typeReference, null, true, listOfSubClassNames,true);
        }

    }

    private void addAutomaticTagsIfNeeded(List<AsnElementType> componentTypes) throws IOException {
        if (tagDefault != TagDefault.AUTOMATIC) {
            return;
        }
        for (AsnElementType element : componentTypes) {
            if (getTag(element) != null) {
                return;
            }
        }
        int i = 0;
        for (AsnElementType element : componentTypes) {
            element.tag = new AsnTag();
            element.tag.classNumber = new AsnClassNumber();
            element.tag.classNumber.num = i;
            i++;
        }

    }


    private boolean isExplicit(Tag tag) {
        return (tag != null) && (tag.type == TagType.EXPLICIT);
    }


    private void writePublicMembers(List<AsnElementType> componentTypes,boolean nested) throws IOException {

        if(!nested) {
            for (AsnElementType element : componentTypes) {
                if (element.className.equals("String")) {
                    AsnSchemaParser.inferredSchema = AsnSchemaParser.inferredSchema.add(
                            new StructField(cleanUpName(element.name), StringType, true, Metadata.empty())
                    );
                } else if (element.className.equals("Integer")) {
                    AsnSchemaParser.inferredSchema = AsnSchemaParser.inferredSchema.add(
                            new StructField(cleanUpName(element.name), IntegerType, true, Metadata.empty())
                    );
                } else if (element.className.equals("Boolean")) {
                    AsnSchemaParser.inferredSchema = AsnSchemaParser.inferredSchema.add(
                            new StructField(cleanUpName(element.name), BooleanType, true, Metadata.empty())
                    );
                } else if (element.className.equals("Real")) {
                    AsnSchemaParser.inferredSchema = AsnSchemaParser.inferredSchema.add(
                            new StructField(cleanUpName(element.name), FloatType, true, Metadata.empty())
                    );
                } else if (element.className.equals("Date")) {
                    AsnSchemaParser.inferredSchema = AsnSchemaParser.inferredSchema.add(
                            new StructField(cleanUpName(element.name), DateType, true, Metadata.empty())
                    );
                } else if (element.className.equals("TimeOfDay")) {
                    AsnSchemaParser.inferredSchema = AsnSchemaParser.inferredSchema.add(
                            new StructField(cleanUpName(element.name), TimestampType, true, Metadata.empty())
                    );
                }
            }
        }else{
            StructType st=new StructType();
            for (AsnElementType element : componentTypes) {
                if (element.className.equals("String")) {
                    st=st.add(
                            new StructField(cleanUpName(element.name), StringType, true, Metadata.empty())
                    );
                } else if (element.className.equals("Integer")) {
                    st=st.add(
                            new StructField(cleanUpName(element.name), IntegerType, true, Metadata.empty())
                    );
                } else if (element.className.equals("Boolean")) {
                    st = st.add(
                            new StructField(cleanUpName(element.name), BooleanType, true, Metadata.empty())
                    );
                } else if (element.className.equals("Real")) {
                    st = st.add(
                            new StructField(cleanUpName(element.name), FloatType, true, Metadata.empty())
                    );
                } else if (element.className.equals("Date")) {
                    st = st.add(
                            new StructField(cleanUpName(element.name), DateType, true, Metadata.empty())
                    );
                } else if (element.className.equals("TimeOfDay")) {
                    st = st.add(
                            new StructField(cleanUpName(element.name), TimestampType, true, Metadata.empty())
                    );
                }
            }
            AsnSchemaParser.inferredSchema= AsnSchemaParser.inferredSchema.add(
                    new StructField("foo", st, true, Metadata.empty()));
        }

    }

    private boolean isInnerType(AsnElementType element) {
        return element.typeReference != null && (element.typeReference instanceof AsnConstructedType);
    }


    private String getClassNameOfSequenceOfElement(AsnElementType componentType, List<String> listOfSubClassNames)
            throws IOException {
        String classNameOfSequenceElement = getClassNameOfSequenceOfElement(componentType);
        for (String subClassName : listOfSubClassNames) {
            if (classNameOfSequenceElement.equals(subClassName)) {
                String moduleName = module.moduleIdentifier.name;

                for (SymbolsFromModule syms : this.module.importSymbolFromModuleList) {
                    if (syms.symbolList.contains(classNameOfSequenceElement)) {
                        moduleName = syms.modref;
                        break;
                    }
                }

                return  sanitizeModuleName(moduleName).replace('-', '.').toLowerCase() + "."
                        + classNameOfSequenceElement;
            }
        }
        return classNameOfSequenceElement;

    }

    private String getClassNameOfSequenceOfElement(AsnElementType componentType) throws IOException {
        if (componentType.typeReference == null) {
            return cleanUpName(componentType.definedType.typeName);
        }
        else {
            AsnType typeDefinition = componentType.typeReference;
            return getClassNameOfSequenceOfTypeReference(typeDefinition);
        }
    }

    private String getClassNameOfSequenceOfTypeReference(AsnType typeDefinition) {
        if (typeDefinition instanceof AsnConstructedType) {

            String subClassName;

            if (typeDefinition instanceof AsnSequenceSet) {

                if (((AsnSequenceSet) typeDefinition).isSequence) {
                    subClassName = "SEQUENCE";
                }
                else {
                    subClassName = "SET";
                }

            }
            else if (typeDefinition instanceof AsnSequenceOf) {
                if (((AsnSequenceOf) typeDefinition).isSequenceOf) {
                    subClassName = "SEQUENCEOF";
                }
                else {
                    subClassName = "SETOF";
                }

            }
            else {
                subClassName = "CHOICE";
            }

            return subClassName;

        }
        return getBerType(typeDefinition);
    }

    private String capitalizeFirstCharacter(String input) {
        return input.substring(0, 1).toUpperCase() + input.substring(1);
    }

    private String getBerType(AsnType asnType) {

        String fullClassName = asnType.getClass().getName();

        String className = fullClassName.substring(fullClassName.lastIndexOf('.') + 1);

        if (className.equals("AsnCharacterString")) {
            AsnCharacterString asnCharacterString = (AsnCharacterString) asnType;
            if (asnCharacterString.stringtype.equals("ISO646String")) {
                return "String";
            }
            else if (asnCharacterString.stringtype.equals("T61String")) {
                return "String";
            }

            else if (asnCharacterString.stringtype.equals("UTF8String")) {
                return "String";
            }


            return ((AsnCharacterString) asnType).stringtype;
        }
        return className.substring(3);
    }

    private AsnType followAndGetNextTaggedOrUniversalType(AsnType asnType, AsnModule module) throws CompileException {
        return followAndGetNextTaggedOrUniversalType(asnType, module, true);
    }

    private AsnType followAndGetNextTaggedOrUniversalType(AsnType asnType, AsnModule module, boolean firstCall)
            throws CompileException {
        if (asnType instanceof AsnTaggedType) {
            if (!firstCall) {
                return asnType;
            }
            AsnTaggedType taggedType = (AsnTaggedType) asnType;
            if (taggedType.definedType != null) {
                return followAndGetNextTaggedOrUniversalType(taggedType.definedType, module, false);
            }
            else {
                return taggedType.typeReference;
            }
        }
        else if (asnType instanceof AsnDefinedType) {

            AsnDefinedType definedType = (AsnDefinedType) asnType;

            if (definedType.isObjectClassField) {

                AsnInformationObjectClass informationObjectClass = getInformationObjectClass(
                        definedType.moduleOrObjectClassReference, module);
                if (informationObjectClass == null) {
                    throw new CompileException("no information object class of name \""
                            + definedType.moduleOrObjectClassReference + "\" found");
                }

                for (AsnElementType elementType : informationObjectClass.elementList) {
                    if (elementType.name.equals(definedType.typeName)) {
                        return followAndGetNextTaggedOrUniversalType(elementType, module, true);
                    }
                }

                throw new IllegalStateException("Could not find field \"" + definedType.typeName
                        + "\" of information object class \"" + definedType.moduleOrObjectClassReference + "\"");
            }
            else {
                return followAndGetNextTaggedOrUniversalType(definedType.typeName, module, false);
            }
        }
        else if (asnType instanceof AsnUniversalType) {
            return asnType;
        }
        else {
            throw new IllegalStateException();
        }

    }

    private AsnType followAndGetNextTaggedOrUniversalType(String typeName, AsnModule module, boolean firstCall)
            throws CompileException {

        AsnType asnType = module.typesByName.get(typeName);
        if (asnType != null) {
            return followAndGetNextTaggedOrUniversalType(asnType, module, false);
        }
        for (SymbolsFromModule symbolsFromModule : module.importSymbolFromModuleList) {
            for (String importedTypeName : symbolsFromModule.symbolList) {
                if (typeName.equals(importedTypeName)) {
                    return followAndGetNextTaggedOrUniversalType(typeName, getAsnModule(symbolsFromModule.modref),
                            false);
                }
            }
        }
        throw new IllegalStateException("Type definition \"" + typeName + "\" was not found in module \""
                + module.moduleIdentifier.name + "\"");

    }

    private AsnModule getAsnModule(String moduleName) {
        AsnModule asnModule = modulesByName.get(moduleName);
        if (asnModule == null) {
            throw new CompileException("Definition of imported module \"" + moduleName + "\" not found.");
        }
        return asnModule;
    }

    private boolean isDirectAnyOrChoice(AsnTaggedType taggedType) throws CompileException {

        AsnType followedType = followAndGetNextTaggedOrUniversalType(taggedType, module);
        return (followedType instanceof AsnAny) || (followedType instanceof AsnChoice);

    }

    private AsnUniversalType getUniversalType(AsnType asnType) throws IOException {
        return getUniversalType(asnType, module);
    }

    private AsnUniversalType getUniversalType(AsnType asnType, AsnModule module) throws IOException {
        while ((asnType = followAndGetNextTaggedOrUniversalType(asnType, module)) instanceof AsnTaggedType) {
        }
        return (AsnUniversalType) asnType;
    }

    private boolean isPrimitive(AsnTaggedType asnTaggedType) throws IOException {
        AsnType asnType = asnTaggedType;
        while ((asnType = followAndGetNextTaggedOrUniversalType(asnType, module)) instanceof AsnTaggedType) {
            if (isExplicit(getTag((AsnTaggedType) asnType))) {
                return false;
            }
        }
        return isPrimitive((AsnUniversalType) asnType);
    }

    private boolean isPrimitive(AsnUniversalType asnType) {
        return !(asnType instanceof AsnConstructedType || asnType instanceof AsnEmbeddedPdv);
    }
}
