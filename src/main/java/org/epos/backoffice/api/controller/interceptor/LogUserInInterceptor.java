package org.epos.backoffice.api.controller.interceptor;

import org.epos.eposdatamodel.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.HandlerInterceptor;
import usermanagementapis.UserGroupManagementAPI;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;


import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class LogUserInInterceptor implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(LogUserInInterceptor.class);


    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws IOException {

        Map<String, String> allRequestParams = convertQueryParameterFromArrayStringToString(request);


        /** TODO: DELETE **/
        /*if (!allRequestParams.containsKey("userId")) {
            allRequestParams.put("userId", "admin");
        }*/


        System.out.println(allRequestParams);

        if (!allRequestParams.containsKey("userId")) {
            String message = "{\"message\": \"The user is not correctly logged in\"}";
            response.setContentType("application/json");
            response.getWriter().write(message);
            response.setStatus(400);
            return false;
        }

        User user = UserGroupManagementAPI.retrieveUserById(allRequestParams.get("userId"));

        System.out.println("RETRIEVED USER: "+user);

        if(user == null){
            user = new User();
            user.setAuthIdentifier(allRequestParams.get("userId"));
            user.setEmail(allRequestParams.get("email"));
            user.setFirstName(allRequestParams.get("firstName"));
            user.setLastName(allRequestParams.get("lastName"));
            user.setIsAdmin(false); //SWITCH when possible to false
            if(UserGroupManagementAPI.createUser(user)){
                user = UserGroupManagementAPI.retrieveUserById(allRequestParams.get("userId"));
                System.out.println("CREATED USER: "+user);
            } else {
                String message = "{\"message\": \"The user doesn't exists\"}";
                response.setContentType("application/json");
                response.getWriter().write(message);
                response.setStatus(400);
                return false;
            }


        }
        System.out.println("FINAL USER: "+user);

        HttpSession session = request.getSession();
        session.setAttribute("user", user);

        return true;
    }

    private Map<String, String> convertQueryParameterFromArrayStringToString(HttpServletRequest request) {
        Map<String, String> allRequestParams = new TreeMap<>();

        Map<String, String[]> parameterMapArrayString = request.getParameterMap();
        for (String valueParamkey : parameterMapArrayString.keySet()) {
            if (parameterMapArrayString.get(valueParamkey).length > 0)
                allRequestParams.put(valueParamkey, parameterMapArrayString.get(valueParamkey)[0]);
        }
        return allRequestParams;
    }

}
