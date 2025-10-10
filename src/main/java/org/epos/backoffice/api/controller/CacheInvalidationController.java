package org.epos.backoffice.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import dao.EposDataModelDAO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.epos.eposdatamodel.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping(
        value = "/invalidate",
        produces = {"application/json"}
)
public class CacheInvalidationController extends MetadataAbstractController<Address> implements ApiDocTag{


    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationController.class);

    @org.springframework.beans.factory.annotation.Autowired
    public CacheInvalidationController(ObjectMapper objectMapper, HttpServletRequest request) {
        super(objectMapper, request, Address.class);
    }

    @RequestMapping(
            produces = {"application/json"},
            method = RequestMethod.POST)
    @Operation(summary = "Invalidates cache", description = "")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The Address instances are correctly retrieved", content = @Content(mediaType = "application/json")),
            @ApiResponse(responseCode = "400", description = "Bad request."),
            @ApiResponse(responseCode = "401", description = "Token is missing or invalid"),
            @ApiResponse(responseCode = "404", description = "Not found"),
            @ApiResponse(responseCode = "415", description = "Wrong media type"),
            @ApiResponse(responseCode = "500", description = "Error executing the request, the error may be, either in the gateway or the backoffice-service")
    })
    public ResponseEntity<?> post(
    ) {
        EposDataModelDAO.getInstance().clearAllCaches();
        return new ResponseEntity<>(HttpStatus.OK);
    }
}