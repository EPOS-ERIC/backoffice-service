package org.epos.backoffice.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import metadataapis.EntityNames;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParameterControllerEntityNameTest {

    @Test
    void resolvesParameterDtoToParameterApiEntityName() {
        ParameterController controller = new ParameterController(new ObjectMapper(), null);

        assertEquals(EntityNames.SOFTWAREAPPLICATIONINPUTPARAMETER, controller.getEntityName());
    }
}
