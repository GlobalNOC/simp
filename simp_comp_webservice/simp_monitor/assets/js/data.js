var url = '../cgi-dir/populate_data.cgi';
var submit_url = '../cgi-dir/simp.cgi';
function loadPage() {


    document.getElementById("rate_box").value = "";
    document.getElementById("rate_box").setAttribute("disabled", true);
    var myRequest = new XMLHttpRequest();
    myRequest.onload = function() {
        console.log(myRequest.response);
        var host_drop = document.getElementById("host_dropdown");
        var group_drop = document.getElementById("group_dropdown");
        var oid_drop = document.getElementById("oid_dropdown");
        var type_drop = document.getElementById("type_dropdown");
        var collapsible = document.getElementById("accordion");
        collapsible.innerHTML = "";
        type_drop.selectedIndex = 0;
        var hostObj= myRequest.response.hosts;
        var groupObj= myRequest.response.groups;

        host_drop.innerHTML = "";
        group_drop.innerHTML = "";
        oid_drop.innerHTML = "";
        var el = document.createElement("option");
        el.textContent = "Please get OIDs";
        el.value = 0;
        oid_drop.appendChild(el);


        var el = document.createElement("option");
        el.textContent = "Please select a host";
        el.value = 0;
        host_drop.appendChild(el);
        for (var x in hostObj) {
            // console.log(hostObj[x]);
            var opt = hostObj[x];
            var el = document.createElement("option");
            el.textContent = opt;
            el.value = opt;
            host_drop.appendChild(el);
        }

        el = document.createElement("option");
        el.textContent = "Please select a group";
        el.value = 0;
        group_drop.appendChild(el);

        for (var x in groupObj) {
            // console.log(groupObj[x]);
            var opt = groupObj[x];
            var el = document.createElement("option");
            el.textContent = opt;
            el.value = opt;
            group_drop.appendChild(el);
        }
    }
    webservice_call(myRequest, url+'?method=get_initial_data&from=simp');
}

function get_data(control) {
    console.log(control.id);
    console.log(control.value);

    var myRequest = new XMLHttpRequest();

    myRequest.onload = function() {
        console.log(myRequest.response);
        var dropdown;
        var respObj;
        var selected_opt;
        if (control.id.indexOf("group") >= 0) {
            dropdown = document.getElementById("host_dropdown");
            selected_opt = dropdown.value;
            respObj= myRequest.response.hosts;
        } else {
            dropdown  = document.getElementById("group_dropdown");
            selected_opt = dropdown.value;
            respObj= myRequest.response.groups;
        }
        dropdown.innerHTML = "";
        for (var x in respObj) {
            console.log(respObj[x]);
            var opt = respObj[x];
            var el = document.createElement("option");
            el.textContent = opt;
            el.value = opt;
            if (opt ==  selected_opt) {
                el.selected = "selected";
            }
            dropdown.appendChild(el);
        }
    }
    if (control.id.indexOf("group") >= 0) {
        webservice_call(myRequest, url+'?method=get_hosts&group='+control.value);
    } else {
        webservice_call(myRequest, url+'?method=get_groups&host='+control.value);

    }

}

function webservice_call(request_object, url) {
    console.log(url);
    request_object.responseType = 'json';
    request_object.open('GET', url, true);
    request_object.send();
}
function change_textbox(control) {
    console.log(control);
    console.log(control.value);


    var rate_box = document.getElementById("rate_box");
    if (control.value == 1) {
        rate_box.value = ""; 
        rate_box.setAttribute("disabled", true);
    } else {
        rate_box.removeAttribute("disabled");
    }
}

function validate(evt) {
    var theEvent = evt || window.event;
    var key = theEvent.keyCode || theEvent.which;
    key = String.fromCharCode( key );
    var regex = /[0-9]|\./;
    if( !regex.test(key) ) {
        theEvent.returnValue = false;
        if(theEvent.preventDefault) theEvent.preventDefault();
    }
}

function getData() {
    var param_str = submit_url +"?";
    var group_drop = document.getElementById('group_dropdown');
    var host_drop = document.getElementById('host_dropdown');
    var oid_drop = document.getElementById('oid_dropdown');

    if (group_drop.value == 0) {
        alert ("Please select a group");
        return false;
    } else if (host_drop.value == 0){
        alert ("Please select a host");
        return false;
    } else if (oid_drop.value == 0) {
        alert ("Please select an OID");
        return false; 
    } else {
        var type_drop = document.getElementById('type_dropdown');
        var rate_text = document.getElementById('rate_box');

        if (type_drop.value == 1) {
            param_str += "method=get" ;
        } else {
            console.log("Rate: " + rate_text.value);
            if (rate_text.value.trim() == "") {
                alert("Please enter the period");
                return false; 
            } 
            param_str += "method=get_rate&period="+rate_text.value;
        }

        param_str += "&host=" + host_drop.value+"&oid="+oid_drop.value;

    }
    console.log(param_str);
    var myRequest = new XMLHttpRequest();
    myRequest.onload = function() {
        // var jsonObjStr= JSON.stringify(myRequest.response);
        console.log(myRequest.response);
        var result = myRequest.response.results;
        var d = document;
        var collapsible = d.getElementById("accordion");
        collapsible.innerHTML = "";
        var count = 0; 
        for (var x in result) {
            console.log(x);
            for (var y in result[x]) {
                var div1 = d.createElement('div');
                div1.setAttribute("class", "panel panel-default");

                var div2 = d.createElement('div');
                div2.setAttribute("class", "panel-heading");

                var title = d.createElement('h4');
                title.setAttribute("class", "panel-title");

                var aTag = d.createElement('a');
                aTag.setAttribute("data-toggle", "collapse");
                aTag.setAttribute("data-parent", "#accordion");
                aTag.setAttribute("href", "#"+count);
                aTag.innerHTML = y;

                title.appendChild(aTag);
                div2.appendChild(title);
                div1.appendChild(div2);

                var div3 = d.createElement('div');
                div3.id = count;
                div3.setAttribute("class", "panel-collapse collapse");

                var div4 = d.createElement('div');
                div4.setAttribute("class", "panel-body");
                var temp_str = "";
                for (var key in result[x][y]) {
                    // temp_str += key + " = " + result[x][y][key] + "<br>";
                    temp_str += key + " = " + (key=="time"?convert(result[x][y][key]):result[x][y][key])+ "<br>";
                }
                div4.innerHTML = temp_str;

                div3.appendChild(div4);
                div1.appendChild(div3);

                collapsible.appendChild(div1);
                count = count + 1; 
            }
        }
    }
    webservice_call(myRequest, param_str);
}
function getOIDs(){
    var group_drop = document.getElementById("group_dropdown");
    if (group_drop.value == 0) {
        alert("Please select a group");
        return false;
    }

    var myRequest = new XMLHttpRequest();
    myRequest.onload = function() {

        var oid_drop = document.getElementById("oid_dropdown");
        var oidObj= myRequest.response.oids;
        console.log(oidObj);
        oid_drop.innerHTML = "";

        var el = document.createElement("option");
        el.textContent = "Please select an oid";
        el.value = 0;
        oid_drop.appendChild(el);
        for (var x in oidObj) {
            var opt = oidObj[x];
            var el = document.createElement("option");
            el.textContent = opt;
            el.value = opt;
            oid_drop.appendChild(el);
        }
    }
    webservice_call(myRequest, url+'?method=get_oids&group='+group_drop.value);
}

