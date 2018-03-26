package com.example.files;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.*;

public class ReadFile {

    public static void main(String[] args) {



        File file = new File("/home/alejandro/Documents/jsonFile.json");
        FileInputStream fis = null;

        try {
            /* fis = new FileInputStream(file);

            System.out.println("Total file size to read (in bytes) : "
                    + fis.available());

            int content;
            while ((content = fis.read()) != -1) {
                // convert to char and display it
                System.out.print((char) content);
            } */

            /* DataFileStream<GenericRecord> reader = new DataFileStream<GenericRecord>(
                    new FileInputStream(file),
                    new GenericDatumReader<GenericRecord>());
                */

            String schemaS =
                    "{" +
                            "   \"type\" : \"record\"," +
                            "   \"name\" : \"Acme\"," +
                            "   \"fields\" : [{ \"name\" : \"username\", \"type\" : \"string\" }," +
                            "   { \"name\" : \"apellidoPaterno\", \"type\" : \"string\"}," +
                            "   { \"name\" : \"apellidoMaterno\", \"type\" : \"string\"}]" +
                            "}";

            String json = "{ \"username\": \"mike\" ," +
                    "   \"apellidoPaterno\" : \"perez\" ," +
                    "   \"apellidoMaterno\" : \"velasco\"" +
                    "}";

            InputStream input2 = new FileInputStream(file);
            InputStream input = new ByteArrayInputStream(json.getBytes());
            DataInputStream din = new DataInputStream(input2);

            Schema schema = Schema.parse(schemaS);

            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

            DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
            Object datum = reader.read(null, decoder);

            System.out.println("Datos datum: "+datum);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fis != null)
                    fis.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }


    }

}

