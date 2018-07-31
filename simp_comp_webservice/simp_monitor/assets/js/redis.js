var url		= "../cgi-dir/populate_data.cgi";
var url_redis	= "../cgi-dir/get_poller_data.cgi";
var key1	= "";
var key0	= "";

function pageLoad(){
    var host_dropdown	= document.getElementById("host-dropdown");
    var xhttp		= new XMLHttpRequest();

    xhttp.onload		=function(){
        console.log(xhttp.response);
        host_dropdown.innerHTML = "";

        var el = document.createElement("option");
        el.textContent = "Please select a Host";
        el.value=0;
        host_dropdown.appendChild(el);

        var hostObj= xhttp.response.hosts;
        for (var x in hostObj){
            var host_name	= hostObj[x];
            var ele		 = document.createElement("option");
            ele.textContent	 = host_name;
            ele.value	 = host_name;
            host_dropdown.appendChild(ele);
        }
    }
    webservice_call(xhttp, url+'?method=get_initial_data&from=redis');
}
function webservice_call(request_object, url) {
    console.log(url);
    request_object.responseType = 'json';
    request_object.open('GET', url, true);
    request_object.send();
}
function get_host_value(){

    document.getElementById("test").innerHTML=document.getElementById("host-dropdown").value;
}
function getGroup(){

    var host_drop	= document.getElementById("host-dropdown");
    var group_drop	= document.getElementById("group-dropdown");
    var xhttp	= new XMLHttpRequest();
    xhttp.onload	=function(){
        console.log(xhttp.response);
        group_drop.innerHTML	= "";

        var el	= document.createElement("option");
        el.textContent	= "Please select a group";
        el.value	=0;
        group_drop.append(el);

        var  groupObj	= xhttp.response.groups;
        for (var x in groupObj){
            var group_name	= groupObj[x];
            var ele	= document.createElement("option");
            ele.textContent	= group_name;
            ele.value	= group_name;
            group_drop.append(ele);
        }
    }
    webservice_call(xhttp, url_redis+"?method=get_groups&ip="+host_drop.value);
}
function get_timestamps(){

    var table_container	= document.getElementById("table-container");
    var host_drop		= document.getElementById("host-dropdown");
    var xhttp		= new XMLHttpRequest();
    var heading		= document.createElement("label");
    var collapsible		= document.getElementById("accordion");

    collapsible.innerHTML	= "";
    heading.style.marginTop 	= "20px";
    heading.style.marginBottom	= "10px";
    heading.setAttribute("class","control-label");
    heading.textContent     = "Please select a  Timestamp";

    table_container.innerHTML	= "";
    table_container.appendChild(heading);

    xhttp.onload	= function(){
        var timestampObj = xhttp.response.groups;
        key0		= xhttp.response.key0;
        var timestamps	= xhttp.response.timestamps
        console.log(xhttp.response);


        var table	= document.createElement("table");
        table.marginLeft= "0px";
        table.setAttribute("class","table table-hover");

        var row		= table.insertRow(0);
        var col1	= row.insertCell(0);
        col1.innerHTML	= "Host Name";
        col1.style.fontWeight = "bold";
        col1.setAttribute("align","center");
        var col2	= row.insertCell(1);
        col2.innerHTML	= "Group";	
        col2.style.fontWeight = "bold";	
        col2.setAttribute("align","center");
        var col3	= row.insertCell(2);
        col3.innerHTML	= "Worker ID";
        col3.style.fontWeight = "bold";
        col3.setAttribute("align","center");
        var col4        = row.insertCell(3);
        col4.innerHTML  = "Timestamp";
        col4.style.fontWeight = "bold";
        col4.setAttribute("align","center");

        table_container.appendChild(table);

        for ( x in timestamps){
            var ip 		= timestamps[x]['ip'];
            var group	= timestamps[x]['group'];
            var worker_id	= timestamps[x]['wid'];
            var timestamp	= timestamps[x]['timestamp'];
            var row         = table.insertRow(1);
            row.onclick	= function() {get_data(ip,group,worker_id,timestamp)};
            var col1        = row.insertCell(0);
            col1.innerHTML  = ip;
            col1.setAttribute("align","center");
            var col2        = row.insertCell(1);
            col2.setAttribute("align","center");
            col2.innerHTML  = group;
            var col3        = row.insertCell(2);
            col3.setAttribute("align","center");
            col3.innerHTML  = worker_id;
            var col4        = row.insertCell(3);
            col4.setAttribute("align","center");
            // col4.innerHTML  = timestamp;
            var converted_timestamp = convert(timestamp);
            // alert(converted_timestamp);
            col4.innerHTML  = converted_timestamp;
        }

    }	
    webservice_call(xhttp, url_redis+"?method=get_timestamp&ip="+host_drop.value);
}
function get_data(ip,group,worker_id,timestamp){
    console.log(ip);
    console.log(group);
    console.log(worker_id);
    console.log(timestamp);	

    var xhttp	= new XMLHttpRequest();
    xhttp.onload	= function(){
        console.log(xhttp.response);
        var data	= xhttp.response.oid;
        var collapsible	= document.getElementById("accordion");
        collapsible.innerHTML	= "";

        var count 	= 0;
        for (var x in data){

            var d	 = document;
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
            aTag.innerHTML = x;

            title.appendChild(aTag);
            div2.appendChild(title);
            div1.appendChild(div2);

            var div3 = d.createElement('div');
            div3.id = count;
            div3.setAttribute("class", "panel-collapse collapse");

            var div4 = d.createElement('div');
            div4.setAttribute("class", "panel-body");
            div4.innerHTML = data[x];

            div3.appendChild(div4);
            div1.appendChild(div3);

            collapsible.appendChild(div1);
            count = count + 1; 

        }

    }
    webservice_call(xhttp,url_redis+"?method=get_data&ip="+ip+"&group_name="+group+"&worker_id="+worker_id+"&timestamp="+timestamp);		

}


function convert(p_timestamp){

    // Unixtimestamp
    // var unixtimestamp = document.getElementById('timestamp').value;

    // Months array
    var months_arr = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

    // Convert timestamp to milliseconds
    var date = new Date(p_timestamp*1000);

    // Year
    var year = date.getFullYear();

    // Month
    var month = months_arr[date.getMonth()];

    // Day
    var day = date.getDate();

    // Hours
    var hours = date.getHours();

    // Minutes
    var minutes = "0" + date.getMinutes();

    // Seconds
    var seconds = "0" + date.getSeconds();

    // Display date time in MM-dd-yyyy h:m:s format
    var convdataTime = month+'-'+day+'-'+year+' '+hours + ':' + minutes.substr(-2) + ':' + seconds.substr(-2) + ' --  '+ p_timestamp;
    return convdataTime;
    // document.getElementById('datetime').innerHTML = convdataTime;

}
