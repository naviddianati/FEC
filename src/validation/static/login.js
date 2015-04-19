

/* Deployed */
url_base = '/FEC/validation/'; 

/* Local */
url_base = ''; 




url_login_verify = url_base + '/login/'
console.log(url_login_verify)


function login_callback(response){
    console.log("response = "+ response);

    if (response == 'SUCCESS'){
        window.location.replace(url_pages + username + "/");
    }
    else{
        greeting_message = $("._greeting_message") 
        greeting_message.hide();
        greeting_message.html("Wrong username and password combination. Please try again.");
        greeting_message.fadeIn();
    }
}


function set_bindings(){
    $("#submit_coder_ID").click(function(){
        username = $("#input_coder_username").val();
        password = $("#input_coder_password").val();
        //var login_data =JSON.stringify({'result':{'username':username,'password':password}});
        login_data ={'username':username,'password':password};
        console.log(login_data);
        callAjax(login_data, url_login_verify,login_callback);
    });

     
     $("#input_coder_username").focus();
     $("#input_coder_password").keypress(function(event){
        if (event.which == 13){
             $("#submit_coder_ID").trigger("click")
        }
      });
     $("#input_coder_ID").keypress(function(event){
        if (event.which == 13){
             $("#submit_coder_ID").trigger("click")
        }
      });
}





function show_greeting_form(){
    $("#greeting_window_container").show();
}





function start_greeting(){
    set_bindings();
    show_greeting_form();
}


