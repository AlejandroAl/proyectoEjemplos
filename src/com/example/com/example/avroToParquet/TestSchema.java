package com.example.com.example.avroToParquet;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.*;

public class TestSchema {

    public static void main(String[] args) {

        Schema schema = null;

        try {

            File file = new File("/home/alejandro/Documents/jsonFile.json");

            InputStream input2 = new FileInputStream(file);
            DataInputStream din = new DataInputStream(input2);



            schema = new Schema.Parser().parse(new File("/home/alejandro/Documents/schemaExample.avsc"));
            System.out.println(schema);

            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            GenericRecord datum = reader.read(null, decoder);
            datum.getSchema();

            File archivoAvro = File.createTempFile("test", ".avro");

            System.out.println("Ruta absoluta: "+archivoAvro.getAbsolutePath());

            DataFileWriter dfileWr = new DataFileWriter<DatumReader>(new GenericDatumWriter<>(datum.getSchema()));

            dfileWr.create(datum.getSchema(),archivoAvro);

            dfileWr.append(datum);


            System.out.println("Datos datum: "+datum);

            System.out.println("Este es el Schema"+datum.getSchema());



        } catch (Exception e) {
            System.out.println(schema);
            e.printStackTrace();
        }

    }

}
