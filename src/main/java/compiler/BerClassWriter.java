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
package compiler;

import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.openmuc.jasn1.ber.types.BerObjectIdentifier;
import org.openmuc.jasn1.compiler.model.*;
import org.openmuc.jasn1.compiler.model.AsnModule.TagDefault;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class BerClassWriter {

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
    private File outputBaseDir;
    private final String basePackageName;
    private int indentNum = 0;

    private final boolean supportIndefiniteLength;
    private final boolean jaxbMode;
    private final HashMap<String, AsnModule> modulesByName;
    private AsnModule module;
    private File outputDirectory;

    BerClassWriter(HashMap<String, AsnModule> modulesByName, String outputBaseDir, String basePackageName,
                   boolean jaxbMode, boolean supportIndefiniteLength) throws IOException {
        this.supportIndefiniteLength = supportIndefiniteLength;
        this.jaxbMode = jaxbMode;
        this.outputBaseDir = new File(outputBaseDir);

        if (basePackageName.isEmpty()) {
            this.basePackageName = "";
        }
        else {
            this.outputBaseDir = new File(this.outputBaseDir, basePackageName.replace('.', '/'));
            this.basePackageName = basePackageName + ".";
        }
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

    int[] toIntArray(List<Integer> list) {
        int[] ret = new int[list.size()];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = list.get(i);
        }
        return ret;
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
                    writeRetaggingTypeClass(typeName, asnTaggedType.definedType.typeName, typeDefinition, tag);
                }
                else {

                    AsnType assignedAsnType = asnTaggedType.typeReference;

                    if (assignedAsnType instanceof AsnConstructedType) {
                        writeConstructedTypeClass(typeName, assignedAsnType, tag, false, null);
                    }
                    else {
                        writeRetaggingTypeClass(typeName, getBerType(assignedAsnType), typeDefinition, tag);
                    }
                }

            }
            else if (typeDefinition instanceof AsnDefinedType) {
                writeRetaggingTypeClass(typeName, ((AsnDefinedType) typeDefinition).typeName, typeDefinition, null);
            }
            else if (typeDefinition instanceof AsnConstructedType) {
                writeConstructedTypeClass(typeName, typeDefinition, null, false, null);
            }
            else {
                writeRetaggingTypeClass(typeName, getBerType(typeDefinition), typeDefinition, null);
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
                                           List<String> listOfSubClassNames) throws IOException {

        if (listOfSubClassNames == null) {
            listOfSubClassNames = new ArrayList<>();
        }

        String isStaticStr = "";
        if (asInternalClass) {
            isStaticStr = " static";
        }

        if (asnType instanceof AsnSequenceSet) {
            writeSequenceOrSetClass(className, (AsnSequenceSet) asnType, tag, isStaticStr, listOfSubClassNames);
        }
        else if (asnType instanceof AsnSequenceOf) {
            writeSequenceOfClass(className, (AsnSequenceOf) asnType, tag, isStaticStr, listOfSubClassNames);
        }
        else if (asnType instanceof AsnChoice) {
            writeChoiceClass(className, (AsnChoice) asnType, tag, isStaticStr, listOfSubClassNames);
        }
    }

    private void writeChoiceClass(String className, AsnChoice asn1TypeElement, Tag tag, String isStaticStr,
                                  List<String> listOfSubClassNames) throws IOException {

        write("public" + isStaticStr + " class " + className + " implements Serializable {\n");

        write("private static final long serialVersionUID = 1L;\n");

        write("public byte[] code = null;");

        if (tag != null) {
            write("public static final BerTag tag = new BerTag(" + getBerTagParametersString(tag) + ");\n");

        }

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
                writeConstructedTypeClass(subClassName, componentType.typeReference, null, true, listOfSubClassNames);

            }
        }

        setClassNamesOfComponents(listOfSubClassNames, componentTypes);

        writePublicMembers(componentTypes);

        writeEmptyConstructor(className);



        if (jaxbMode) {
            writeGetterAndSetter(componentTypes);
        }

        write("}\n");

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

                        return basePackageName + sanitizeModuleName(moduleName).replace('-', '.').toLowerCase() + "."
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
                                         List<String> listOfSubClassNames) throws IOException {

        write("public" + isStaticStr + " class " + className + " implements Serializable {\n");

        write("private static final long serialVersionUID = 1L;\n");

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

                writeConstructedTypeClass(subClassName, componentType.typeReference, null, true, listOfSubClassNames);

            }

        }

        Tag mainTag;
        if (tag == null) {
            if (asnSequenceSet.isSequence) {
                mainTag = stdSeqTag;
            }
            else {
                mainTag = stdSetTag;
            }
        }
        else {
            mainTag = tag;
        }

        write("public static final BerTag tag = new BerTag(" + getBerTagParametersString(mainTag) + ");\n");

        write("public byte[] code = null;");

        setClassNamesOfComponents(listOfSubClassNames, componentTypes);

        writePublicMembers(componentTypes);

        writeEmptyConstructor(className);



        if (jaxbMode) {
            writeGetterAndSetter(componentTypes);
        }

        boolean hasExplicitTag = (tag != null) && (tag.type == TagType.EXPLICIT);

        write("}\n");

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




    private void writeSequenceOfClass(String className, AsnSequenceOf asnSequenceOf, Tag tag, String isStaticStr,
                                      List<String> listOfSubClassNames) throws IOException {

        write("public" + isStaticStr + " class " + className + " implements Serializable {\n");

        write("private static final long serialVersionUID = 1L;\n");

        AsnElementType componentType = asnSequenceOf.componentType;

        String referencedTypeName = getClassNameOfSequenceOfElement(componentType, listOfSubClassNames);

        if (isInnerType(componentType)) {
            writeConstructedTypeClass(referencedTypeName, componentType.typeReference, null, true, listOfSubClassNames);
        }

        Tag mainTag;
        if (tag == null) {
            if (asnSequenceOf.isSequenceOf) {
                mainTag = stdSeqTag;
            }
            else {
                mainTag = stdSetTag;
            }
        }
        else {
            mainTag = tag;
        }

        write("public static final BerTag tag = new BerTag(" + getBerTagParametersString(mainTag) + ");");

        write("public byte[] code = null;");

        if (jaxbMode) {
            write("private List<" + referencedTypeName + "> seqOf = null;\n");
        }
        else {
            write("public List<" + referencedTypeName + "> seqOf = null;\n");
        }

        write("public " + className + "() {");
        write("seqOf = new ArrayList<" + referencedTypeName + ">();");
        write("}\n");

        write("public " + className + "(byte[] code) {");
        write("this.code = code;");
        write("}\n");

        if (!jaxbMode) {
            write("public " + className + "(List<" + referencedTypeName + "> seqOf) {");
            write("this.seqOf = seqOf;");
            write("}\n");
        }

        if (jaxbMode) {
            writeGetterForSeqOf(referencedTypeName);
        }

        boolean hasExplicitTag = (tag != null) && (tag.type == TagType.EXPLICIT);

        write("}\n");

    }

    private void writeRetaggingTypeClass(String typeName, String assignedTypeName, AsnType typeDefinition, Tag tag)
            throws IOException {

        write("public class " + typeName + " extends " + cleanUpName(assignedTypeName) + " {\n");

        write("private static final long serialVersionUID = 1L;\n");

        if (tag != null) {

            write("public static final BerTag tag = new BerTag(" + getBerTagParametersString(tag) + ");\n");

            if (tag.type == TagType.EXPLICIT) {
                write("public byte[] code = null;\n");
            }
        }

        write("public " + typeName + "() {");
        write("}\n");

        String[] constructorParameters = getConstructorParameters(getUniversalType(typeDefinition));

        if (constructorParameters.length != 2 || constructorParameters[0] != "byte[]") {
            write("public " + typeName + "(byte[] code) {");
            write("super(code);");
            write("}\n");
        }

        if ((!jaxbMode || isPrimitiveOrRetaggedPrimitive(typeDefinition)) && (constructorParameters.length != 0)) {
            String constructorParameterString = "";
            String superCallParameterString = "";
            for (int i = 0; i < constructorParameters.length; i += 2) {
                if (i > 0) {
                    constructorParameterString += ", ";
                    superCallParameterString += ", ";
                }
                constructorParameterString += constructorParameters[i] + " " + constructorParameters[i + 1];
                superCallParameterString += constructorParameters[i + 1];
            }

            write("public " + typeName + "(" + constructorParameterString + ") {");
            write("super(" + superCallParameterString + ");");
            write("}\n");

            if (constructorParameters[0].equals("BigInteger")) {
                write("public " + typeName + "(long value) {");
                write("super(value);");
                write("}\n");
            }

        }

        if (tag != null) {



            write("public int encode(BerByteArrayOutputStream os, boolean withTag) throws IOException {\n");

            if (constructorParameters.length != 2 || constructorParameters[0] != "byte[]") {
                write("if (code != null) {");
                write("for (int i = code.length - 1; i >= 0; i--) {");
                write("os.write(code[i]);");
                write("}");
                write("if (withTag) {");
                write("return tag.encode(os) + code.length;");
                write("}");
                write("return code.length;");
                write("}\n");
            }

            write("int codeLength;\n");

            if (tag.type == TagType.EXPLICIT) {
                if (isDirectAnyOrChoice((AsnTaggedType) typeDefinition)) {
                    write("codeLength = super.encode(os);");
                }
                else {
                    write("codeLength = super.encode(os, true);");
                }
                write("codeLength += BerLength.encodeLength(os, codeLength);");
            }
            else {
                write("codeLength = super.encode(os, false);");
            }

            write("if (withTag) {");
            write("codeLength += tag.encode(os);");
            write("}\n");

            write("return codeLength;");
            write("}\n");



            write("public int decode(InputStream is, boolean withTag) throws IOException {\n");

            write("int codeLength = 0;\n");

            write("if (withTag) {");
            write("codeLength += tag.decodeAndCheck(is);");
            write("}\n");

            if (tag.type == TagType.EXPLICIT) {

                write("BerLength length = new BerLength();");
                write("codeLength += length.decode(is);\n");

                if (isDirectAnyOrChoice((AsnTaggedType) typeDefinition)) {
                    write("codeLength += super.decode(is, null);\n");
                }
                else {
                    write("codeLength += super.decode(is, true);\n");
                }
            }
            else {
                write("codeLength += super.decode(is, false);\n");
            }

            write("return codeLength;");
            write("}\n");
        }

        write("}");

    }


    private static String getBerTagParametersString(Tag tag) {
        return "BerTag." + tag.tagClass + "_CLASS, BerTag." + tag.typeStructure.toString() + ", " + tag.value;
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



    private void writeEmptyConstructor(String className) throws IOException {
        write("public " + className + "() {");
        write("}\n");

        write("public " + className + "(byte[] code) {");
        write("this.code = code;");
        write("}\n");
    }

    private void writePublicMembers(List<AsnElementType> componentTypes) throws IOException {
        for (AsnElementType element : componentTypes) {

                if(element.className .equals("String")) {


                    InferSchema.inferredSchema=InferSchema.inferredSchema.add(
                            new StructField(cleanUpName(element.name), StringType, true, Metadata.empty())


                    );
                }else if(element.className .equals("Integer")){
                    InferSchema.inferredSchema=InferSchema.inferredSchema.add(
                            new StructField(cleanUpName(element.name), IntegerType, true, Metadata.empty())


                    );
                }

        }

    }

    private boolean isInnerType(AsnElementType element) {
        return element.typeReference != null && (element.typeReference instanceof AsnConstructedType);
    }

    private void writeGetterAndSetter(List<AsnElementType> componentTypes) throws IOException {
        for (AsnElementType element : componentTypes) {
            String typeName = getClassNameOfComponent(element);
            String getterName = cleanUpName("get" + capitalizeFirstCharacter(element.name));
            String setterName = cleanUpName("set" + capitalizeFirstCharacter(element.name));
            String variableName = cleanUpName(element.name);
            write("public void " + setterName + "(" + typeName + " " + variableName + ") {");
            write("this." + variableName + " = " + variableName + ";");
            write("}\n");
            write("public " + typeName + " " + getterName + "() {");
            write("return " + variableName + ";");
            write("}\n");
        }
    }

    private void writeGetterForSeqOf(String referencedTypeName) throws IOException {
        write("public List<" + referencedTypeName + "> get"
                + referencedTypeName.substring(referencedTypeName.lastIndexOf('.') + 1) + "() {");
        write("if (seqOf == null) {");
        write("seqOf = new ArrayList<" + referencedTypeName + ">();");
        write("}");
        write("return seqOf;");
        write("}\n");
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

                return basePackageName + sanitizeModuleName(moduleName).replace('-', '.').toLowerCase() + "."
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

    private String[] getConstructorParameters(AsnUniversalType typeDefinition) throws IOException {

        if (typeDefinition instanceof AsnInteger || typeDefinition instanceof AsnEnum) {
            return new String[] { "BigInteger", "value" };
        }
        else if (typeDefinition instanceof AsnReal) {
            return new String[] { "double", "value" };
        }
        else if (typeDefinition instanceof AsnBoolean) {
            return new String[] { "boolean", "value" };
        }
        else if (typeDefinition instanceof AsnObjectIdentifier) {
            return new String[] { "int[]", "value" };
        }
        else if (typeDefinition instanceof AsnBitString) {
            return new String[] { "byte[]", "value", "int", "numBits" };
        }
        else if (typeDefinition instanceof AsnOctetString || typeDefinition instanceof AsnCharacterString) {
            return new String[] { "byte[]", "value" };
        }
        else if (typeDefinition instanceof AsnNull) {
            return new String[0];
        }
        else if ((typeDefinition instanceof AsnSequenceSet) || (typeDefinition instanceof AsnChoice)) {
            return getConstructorParametersFromConstructedElement((AsnConstructedType) typeDefinition);
        }
        else if (typeDefinition instanceof AsnSequenceOf) {
            return new String[] {
                    "List<" + getClassNameOfSequenceOfElement(((AsnSequenceOf) typeDefinition).componentType) + ">",
                    "seqOf" };
        }
        else if (typeDefinition instanceof AsnAny) {
            return new String[] { "byte[]", "value" };
        }
        else if (typeDefinition instanceof AsnEmbeddedPdv) {
            return new String[0];
        }
        else {
            throw new IllegalStateException("type of unknown class: " + typeDefinition.name);
        }

    }

    private String[] getConstructorParametersFromConstructedElement(AsnConstructedType assignedTypeDefinition)
            throws IOException {

        List<AsnElementType> componentTypes;

        if (assignedTypeDefinition instanceof AsnSequenceSet) {

            componentTypes = ((AsnSequenceSet) assignedTypeDefinition).componentTypes;
        }
        else {
            componentTypes = ((AsnChoice) assignedTypeDefinition).componentTypes;
        }

        String[] constructorParameters = new String[componentTypes.size() * 2];

        for (int j = 0; j < componentTypes.size(); j++) {
            AsnElementType componentType = componentTypes.get(j);

            constructorParameters[j * 2] = getClassNameOfComponent(componentType);
            constructorParameters[j * 2 + 1] = cleanUpName(componentType.name);

        }
        return constructorParameters;
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

    private boolean isPrimitiveOrRetaggedPrimitive(AsnType asnType) throws IOException {
        return isPrimitive(getUniversalType(asnType));
    }

    private boolean isPrimitive(AsnUniversalType asnType) {
        return !(asnType instanceof AsnConstructedType || asnType instanceof AsnEmbeddedPdv);
    }


    private void write(String line) throws IOException {

    }

}
