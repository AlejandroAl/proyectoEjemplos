package com.example.com.example.avroToParquet;

        import java.io.*;

        import org.apache.avro.Schema;
        import org.apache.avro.file.DataFileReader;
        import org.apache.avro.generic.GenericDatumReader;
        import org.apache.avro.generic.GenericRecord;
        import org.apache.avro.io.DatumReader;
        import org.apache.avro.io.Decoder;
        import org.apache.avro.io.DecoderFactory;
        import org.apache.hadoop.fs.Path;

        import parquet.avro.AvroParquetWriter;
        import parquet.hadoop.ParquetWriter;
        import parquet.hadoop.metadata.CompressionCodecName;

        import org.apache.commons.cli.CommandLine;
        import org.apache.commons.cli.CommandLineParser;
        import org.apache.commons.cli.DefaultParser;
        import org.apache.commons.cli.HelpFormatter;
        import org.apache.commons.cli.Options;
        import org.apache.commons.cli.ParseException;
        import org.apache.commons.io.FilenameUtils;


public class ChangeDataWithFiles {

    public static void main(String[] args) {

        Schema schema = null;

        String inputFile = "/home/alejandro/Documents/twitter.avro";
        String outputFile = "/home/alejandro/Documents/twitter2.parquet";

        HelpFormatter formatter = new HelpFormatter();
        // create Options object
        Options options = new Options();

        // add t option
        options.addOption("i", true, "input avro file");
        options.addOption("o", true, "output Parquet file");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            //inputFile = cmd.getOptionValue("i");
            if (inputFile == null) {
                formatter.printHelp("AvroToParquet", options);
                return;
            }
            //outputFile = cmd.getOptionValue("o");
        } catch (ParseException exc) {
            System.err.println("Problem with command line parameters: " + exc.getMessage());
            return;
        }

        File avroFile = new File(inputFile);

        if (!avroFile.exists()) {
            System.err.println("Could not open file: " + inputFile);
            return;
        }
        try {

            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
            File file2 = new File("/home/alejandro/Documents/jsonFile.json");
            InputStream input2 = new FileInputStream(file2);
            DataInputStream din = new DataInputStream(input2);



            schema = new Schema.Parser().parse(new File("/home/alejandro/Documents/schemaExample.avsc"));
            System.out.println(schema);

            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            GenericRecord datum = reader.read(null, decoder);


            Schema avroSchema = datum.getSchema();

            // choose compression scheme
            CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;

            // set Parquet file block size and page size values
            int blockSize = 256 * 1024 * 1024;
            int pageSize = 64 * 1024;

            String base = FilenameUtils.removeExtension(avroFile.getAbsolutePath()) + ".parquet";
            if(outputFile != null) {
                File file = new File(outputFile);
                base = file.getAbsolutePath();
            }

            Path outputPath = new Path("file:///"+base);

            // the ParquetWriter object that will consume Avro GenericRecords
            ParquetWriter<GenericRecord> parquetWriter;
            parquetWriter = new AvroParquetWriter<GenericRecord>(outputPath, avroSchema, compressionCodecName, blockSize, pageSize);

            parquetWriter.write(datum);
            parquetWriter.close();

        } catch (IOException e) {
            System.err.println("Caught exception: " + e.getMessage());
        }

    }
}