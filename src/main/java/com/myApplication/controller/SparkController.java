package com.myApplication.controller;

import com.myApplication.service.AvroConversion;
import com.myApplication.service.CsvConversion;
import com.myApplication.service.TextConversion;
import com.myApplication.service.impl.AvroConversionImpl;
import com.myApplication.service.impl.CsvConversionImpl;
import com.myApplication.service.impl.ParquetConversionImpl;
import com.myApplication.service.ParquetConversion;
import com.myApplication.service.impl.TextConversionImpl;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController("/spark")
public class SparkController {

    @PostMapping("/convertToParquet")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The POST call is Successful"),
            @ApiResponse(code = 500, message = "The POST call is Failed"),
            @ApiResponse(code = 404, message = "The API could not be found")
    })
    public ResponseEntity<String> ConvertToParquet(@RequestParam String inputPath,
                                                          @RequestParam String outputPath,
                                                          @RequestParam String fileFormat) {

        try {

            ParquetConversion parquetConversion = new ParquetConversionImpl();
            parquetConversion.convertToParquet(inputPath, outputPath, fileFormat);

            if(checkIfOutputGenerated(outputPath))
                return new ResponseEntity(HttpStatus.OK);
            else
                return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);

        } catch (IOException ioe) {
            System.err.println("IOException occurred::: "+ioe);
        }
        return null;
    }

    @PostMapping("/convertToCsv")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The POST call is Successful"),
            @ApiResponse(code = 500, message = "The POST call is Failed"),
            @ApiResponse(code = 404, message = "The API could not be found")
    })
    public ResponseEntity<String> ConvertToCsv(@RequestParam String inputPath,
                                                   @RequestParam String outputPath,
                                                   @RequestParam String fileFormat) {

        try {

            CsvConversion csvConversion = new CsvConversionImpl();
            csvConversion.convertToCsv(inputPath, outputPath, fileFormat);

            if(checkIfOutputGenerated(outputPath))
                return new ResponseEntity(HttpStatus.OK);
            else
                return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);

        } catch (IOException ioe) {
            System.err.println("IOException occurred::: "+ioe);
        }
        return null;
    }

    @PostMapping("/convertToAvro")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The POST call is Successful"),
            @ApiResponse(code = 500, message = "The POST call is Failed"),
            @ApiResponse(code = 404, message = "The API could not be found")
    })
    public ResponseEntity<String> ConvertToAvro(@RequestParam String inputPath,
                                               @RequestParam String outputPath,
                                               @RequestParam String fileFormat) {

        try {

            AvroConversion avroConversion = new AvroConversionImpl();
            avroConversion.convertToAvro(inputPath, outputPath, fileFormat);

            if(checkIfOutputGenerated(outputPath))
                return new ResponseEntity(HttpStatus.OK);
            else
                return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);

        } catch (IOException ioe) {
            System.err.println("IOException occurred::: "+ioe);
        }
        return null;
    }

    @PostMapping("/convertToText")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The POST call is Successful"),
            @ApiResponse(code = 500, message = "The POST call is Failed"),
            @ApiResponse(code = 404, message = "The API could not be found")
    })
    public ResponseEntity<String> ConvertToText(@RequestParam String inputPath,
                                                @RequestParam String outputPath,
                                                @RequestParam String fileFormat,
                                                @RequestParam String delimiter) {

        try {

            TextConversion textConversion = new TextConversionImpl();
            textConversion.convertToText(inputPath, outputPath, fileFormat, delimiter);

            if(checkIfOutputGenerated(outputPath))
                return new ResponseEntity(HttpStatus.OK);
            else
                return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);

        } catch (IOException ioe) {
            System.err.println("IOException occurred::: "+ioe);
        }
        return null;
    }

    private boolean checkIfOutputGenerated(String outputPath) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);

        Path outputDir = new Path(outputPath+"/_SUCCESS");

        if(fileSystem.exists(outputDir))
            return true;
        else
            return false;

    }
}
