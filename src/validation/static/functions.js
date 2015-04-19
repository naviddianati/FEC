//var current_form = -1l
// variable num_of_forms is already set in turk.py

/* Deployed */
url_base = '/FEC/validation/'; 

/* Local */
url_base = ''; 


url_submit = url_base + '/submit/';  
url_pages = url_base + '/page/'; 

session_id = 0;

function keyboard_lock(){
    $(document).locked = true;
}



function keyboard_unlock(){
    $(document).locked = false;
}


function show_next_form(current_form){
        $(document).unbind('keypress');
        var n = current_form + 1;
        var button_A = $("#button-A-"+n);
        var button_B = $("#button-B-"+n);
        var button_C = $("#button-C-"+n);
        $("._counter").show();
        $("._counter").text((1+n)+'/'+num_of_forms);
        // if current is not the first page, hide the previous page and save the response
        if (n >= 1){
          
            $('#form-'+(n-1)).hide();
        }
        if (n==0)
            $('#form-'+n).fadeIn();
        else
            $('#form-'+n).show();

    if (current_form == num_of_forms-2 ){
        // activate the final form
        $("#button-A-"+n).click(function(){
            if (!$(document).locked){
                keyboard_lock();
                $(document).unbind('keypress');
                save_response("A",n)
                submit_form();
                keyboard_unlock();
            }
        });
        $("#button-B-"+n).click(function(){
            if (!$(document).locked){
                keyboard_lock();
                $(document).unbind('keypress');
                save_response("B",n)
                submit_form();
                keyboard_unlock();
            }
        });
        $("#button-C-"+n).click(function(){
            if (!$(document).locked){
                keyboard_lock();
                $(document).unbind('keypress');
                save_response("C",n)
                submit_form();
                keyboard_unlock();
            }
        });
    }
    else{
        // activate the next regular form
        button_A.data('n',n);
        button_A.click(function(){
            if (!(document).locked){
                var n =$(this).data('n'); 
                keyboard_lock();
                save_response("A",n);
                show_next_form(n);
                keyboard_unlock();
            }
        });
        button_B.data('n',n);
        button_B.click(function(){
            if (!$(document).locked){
                var n =$(this).data('n'); 
                keyboard_lock();
                save_response("B",n);
                show_next_form(n);
                keyboard_unlock();
            }
        });
        button_C.data('n',n);
        button_C.click(function(){
            if (!$(document).locked){
                var n =$(this).data('n'); 
                keyboard_lock();
                save_response("C",n);
                show_next_form(n);
                keyboard_unlock();
            }
        });



        
    }   
    
    $(document).keypress(function(event){
        //$(".debug").html(event.which());
        if (!$(document).locked){
                if (event.which == 106){
                //    $(".debug").html(''+event.which);
                    button_A.fadeTo(100,0.5).queue(function(next) {
                            $(this).trigger("click")
                            next();
                    });
        //            $("#button-A-"+n).click();
                }
                if (event.which == 107){
                    button_B.fadeTo(100,0.5).queue(function(next) {
                            $(this).trigger("click")
                            next();
                    });
                }
                if (event.which == 108){
                    button_C.fadeTo(100,0.5).queue(function(next) {
                            $(this).trigger("click")
                            next();
                    });
                }
        }

    });



}











function callAjax(data,url, callback){
    my_delay = 1000;
    $.ajax({
        url: url,
        type: "POST",
        data: {"post":JSON.stringify(data)},
        dataType: "text",
        tryCount : 0,
        retryLimit : 5,
        success: callback
           /* response = response_text;
            console.log('Ajax request returned successfully.');
            if (response == "SUCCESS")
                if (url_success != '')
                    window.location.replace(url_success);
                                
        }*/ 
        
        /*,
        error : function(xhr, textStatus, errorThrown ) {
                console.log('Ajax request failed.');
                if (textStatus == 'timeout') {
                    this.tryCount++;
                    if (this.tryCount <= this.retryLimit) {
                        $.ajax(this);
                        return;
                    }            
                    return;
                }
                if (xhr.status == 404) {
                    this.tryCount++;
                    if (this.tryCount <= this.retryLimit) {
                        setTimeout(callAjax, my_delay);
                        return;
                    }            
                    return;
                } 
                else{
                    this.tryCount++;
                    if (this.tryCount <= this.retryLimit) {
                        setTimeout(callAjax, my_delay);
                        return;
                    }            
                    return;
                } 
        }*/
    });
   
    
}



/* a dictionary storing the current state of
all rows. 0 is unselected, 1 is selected. */
row_stats = {};
pageno = 0;
username = '';
debug = {};



function get_score_form(x,y,id,initial_state){
   var score_form = 
    $('<div/>', {
    class:"score-form",
    id: 'score-form-' + id,
    //href: 'http://google.com',
    //title: 'Become a Googler',
    ///rel: 'external',
    //text: 'Go to Google!'
    });


    var form = $('<form/>');
    var option1 = $('<input/>', {'type':'radio', 'name': 'score', 'value':'yes','class':'score-radio-button'});
    var option2 = $('<input/>', {'type':'radio', 'name': 'score', 'value':'maybeyes','class':'score-radio-button'});
    var option3 = $('<input/>', {'type':'radio', 'name': 'score', 'value':'maybeno','class':'score-radio-button'});
    var option4 = $('<input/>', {'type':'radio', 'name': 'score', 'value':'no','class':'score-radio-button'});

    console.log(initial_state);

    switch (initial_state){
        case "score-yes": 
            option1.attr('checked', 'checked');
            break;
        case "score-maybeyes": 
            option2.attr('checked', 'checked');
            break;
        case "score-maybeno": 
            option3.attr('checked', 'checked');
            break;
        case "score-no": 
            option4.attr('checked', 'checked');
            break;
    }
        
    
    option1.click(function(e){
        // Update the class of the corresponding table row.
        $('#'+id).removeClass("score-yes score-maybeyes score-maybeno score-no");
        $('#'+id).addClass("score-yes");
        $('#score-form-' + id).remove(); 
        $('#'+id).removeClass('selected');
    });
    option2.click(function(e){
        // Update the class of the corresponding table row.
        $('#'+id).removeClass("score-yes score-maybeyes score-maybeno score-no");
        $('#'+id).addClass("score-maybeyes");
        $('#score-form-' + id).remove(); 
        $('#'+id).removeClass('selected');
    });
    option3.click(function(e){
        // Update the class of the corresponding table row.
        $('#'+id).removeClass("score-yes score-maybeyes score-maybeno score-no");
        $('#'+id).addClass("score-maybeno");
        $('#score-form-' + id).remove(); 
        $('#'+id).removeClass('selected');
    });
    option4.click(function(e){
        // Update the class of the corresponding table row.
        $('#'+id).removeClass("score-yes score-maybeyes score-maybeno score-no");
        $('#'+id).addClass("score-no");
        $('#score-form-' + id).remove(); 
        $('#'+id).removeClass('selected');
    });


    
    var label1 = $('<div/>', {'text':'Yes', 'class':'score-label'});
    var label2 = $('<div/>', {'text':'Maybe Yes', 'class':'score-label'});
    var label3 = $('<div/>', {'text':'Maybe No', 'class':'score-label'});
    var label4 = $('<div/>', {'text':'No', 'class':'score-label'});

    var container_main = $('<div/>' , {'class':'score-input-wrapper'});
    container_main.appendTo(form);
 
    var container1 = $('<div/>' , {'class':'score-input-container-1'});
    var container2 = $('<div/>' , {'class':'score-input-container-2'});
    var container3 = $('<div/>' , {'class':'score-input-container-3'});
    var container4 = $('<div/>' , {'class':'score-input-container-4'});
    
    container1.click(function(e){option1.trigger('click');});
    container2.click(function(e){option2.trigger('click');});
    container3.click(function(e){option3.trigger('click');});
    container4.click(function(e){option4.trigger('click');});

    option1.appendTo(container1); label1.appendTo(container1); container1.appendTo(container_main); 
    option2.appendTo(container2); label2.appendTo(container2); container2.appendTo(container_main); 
    option3.appendTo(container3); label3.appendTo(container3); container3.appendTo(container_main); 
    option4.appendTo(container4); label4.appendTo(container4); container4.appendTo(container_main); 
   
    form.appendTo(score_form);
    form.show();
     
    score_form.css({"top" : (y-10) ,"left" : (x-10) }); 
    score_form.mouseleave(function(e){
        $(this).hide();        
        $(this).remove();
        $('#'+id).removeClass('selected');
    });
    score_form.appendTo('html'); 
    score_form.show();
    console.log(""+ x + "," + y);
    
}

function initialize(n,u){
    pageno = n;
    username = u;
    debug = $("#debug");
    num_records = $(".record").length;


    $(".record").click(function(e){
                var mouse_x = e.pageX; 
                var mouse_y = e.pageY; 
                var row_top = $(this).position().top; 
                var id = $(this).attr('id');
                
                $(this).addClass('selected');
                var current_state;
                if ($(this).hasClass('score-yes'))
                    current_state = "score-yes";
                    
                if ($(this).hasClass('score-maybeyes'))
                    current_state = "score-maybeyes";
                if ($(this).hasClass('score-maybeno'))
                    current_state = "score-maybeno";
                if ($(this).hasClass('score-no'))
                    current_state = "score-no";
                    
                get_score_form(mouse_x,mouse_y,id,current_state);
                //$(this).toggleClass("selected");
    });

    $("#button-submit").click(function(){
       results = compile_data();
       callAjax(results, url_submit,submit_callback);
    });

    $("#back-button").click(function(){
       results = compile_data();
       results["goback"] = true;
       callAjax(results, url_submit, submit_callback );
    });
}


function submit_callback(response){
    if (response == "SUCCESS")
        window.location.replace( url_pages +  username + "/");
}



function compile_data(){
    records = $(".record");
    var result_data = {'username': username, 'pageno': pageno, 'data':{} , 'comment':''};
    records.each(function(index,element){
        var id = $(this).find(".id").text();
        var identity = $(this).find(".identity").text();
        var is_focus = 0+$(this).hasClass("ismain");
        //var is_selected = 0+$(this).hasClass("selected");
        var current_state;
            if ($(this).hasClass('score-yes'))
                current_state = 1;
                
            if ($(this).hasClass('score-maybeyes'))
                current_state = 2;
            if ($(this).hasClass('score-maybeno'))
                current_state = 3;
            if ($(this).hasClass('score-no'))
                current_state = 4;
        
        result_data['data'][id] = [identity,is_focus,current_state]; 
        //console.log(id,identity,is_focus,is_selected);
    });
    var comment_text = $("#comments_textarea").val();
    result_data['comment'] = comment_text;
    //console.log(result_data);

    return result_data;
    //console.log(data);
}



