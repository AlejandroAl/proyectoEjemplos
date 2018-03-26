package com.example;

import org.kitesdk.data.Datasets;

import java.net.URI;
import java.net.URISyntaxException;

public class URISchema {

    public static void main(String[] args) {

        String strSchema = "dataset:hdfs:/tmp/data/users";

        URI uri = null;

        try {
            uri = new URI(strSchema);
            System.out.println(uri.getScheme());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        System.out.println(Datasets.load(uri).getDataset().getDescriptor().getSchema());

    }

}