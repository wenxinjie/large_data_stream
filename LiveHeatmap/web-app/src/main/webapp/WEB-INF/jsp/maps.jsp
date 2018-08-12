<%--
  Created by IntelliJ IDEA.
  User: zyfei
  Date: 4/19/18
  Time: 19:20
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Heatmaps</title>
    <style>
        /* Always set the map height explicitly to define the size of the div
         * element that contains the map. */
        #map {
            height: 100%;
        }
        /* Optional: Makes the sample page fill the window. */
        html, body {
            height: 100%;
            margin: 0;
            padding: 0;
        }
        #floating-panel {
            position: absolute;
            top: 10px;
            left: 25%;
            z-index: 5;
            background-color: #fff;
            padding: 5px;
            border: 1px solid #999;
            text-align: center;
            font-family: 'Roboto','sans-serif';
            line-height: 30px;
            padding-left: 10px;
        }
        #floating-panel {
            background-color: #fff;
            border: 1px solid #999;
            left: 25%;
            padding: 5px;
            position: absolute;
            top: 10px;
            z-index: 5;
        }
    </style>
</head>

<body>
<div id="floating-panel">
    <button onclick="toggleHeatmap()">Toggle Heatmap</button>
    <button onclick="changeGradient()">Change gradient</button>
    <button onclick="changeRadius()">Change radius</button>
    <button onclick="changeOpacity()">Change opacity</button>
</div>
<div id="map"></div>
<script>

    // This example requires the Visualization library. Include the libraries=visualization
    // parameter when you first load the API. For example:
    // <script src="https://maps.googleapis.com/maps/api/js?key=YOUR_API_KEY&libraries=visualization">

    var map, heatmap;

    var mapStyle = [{
        'featureType': 'all',
        'elementType': 'all',
        'stylers': [{'visibility': 'off'}]
    }, {
        'featureType': 'landscape',
        'elementType': 'geometry',
        'stylers': [{'visibility': 'on'}, {'color': '#fcfcfc'}]
    }, {
        'featureType': 'water',
        'elementType': 'labels',
        'stylers': [{'visibility': 'off'}]
    }, {
        'featureType': 'water',
        'elementType': 'geometry',
        'stylers': [{'visibility': 'on'}, {'hue': '#5f94ff'}, {'lightness': 60}]
    }];

    function initMap() {
        map = new google.maps.Map(document.getElementById('map'), {
            zoom: 4,
            center: {lat: 41.22, lng: -100.68},
            //mapTypeId: 'roadmap'
            //styles: mapStyle
            mapTypeId: 'satellite'
        });

        heatmap = new google.maps.visualization.HeatmapLayer({
            data: getPoints(),
            map: map
        });

        heatmap.set('radius', 15);
    }

    function toggleHeatmap() {
        heatmap.setMap(heatmap.getMap() ? null : map);
    }

    function changeGradient() {
        var gradient = [
            'rgba(0, 255, 255, 0)',
            'rgba(0, 255, 255, 1)',
            'rgba(0, 191, 255, 1)',
            'rgba(0, 127, 255, 1)',
            'rgba(0, 63, 255, 1)',
            'rgba(0, 0, 255, 1)',
            'rgba(0, 0, 223, 1)',
            'rgba(0, 0, 191, 1)',
            'rgba(0, 0, 159, 1)',
            'rgba(0, 0, 127, 1)',
            'rgba(63, 0, 91, 1)',
            'rgba(127, 0, 63, 1)',
            'rgba(191, 0, 31, 1)',
            'rgba(255, 0, 0, 1)'
        ]
        heatmap.set('gradient', heatmap.get('gradient') ? null : gradient);
    }

    function changeRadius() {
        heatmap.set('radius', heatmap.get('radius') ? null : 20);
    }

    function changeOpacity() {
        heatmap.set('opacity', heatmap.get('opacity') ? null : 0.2);
    }

    // Heatmap data: 500 Points
    function getPoints() {

        points = ${data_json};
        len = points.length;

        coord = new Array(len);

        for (i=0; i<len; i++) {
            coord[i] = {location: new google.maps.LatLng(points[i]["lat"], points[i]["lng"]), weight: points[i]["count"]}
        }

        console.log(coord);
        point = new google.maps.LatLng(34, -122);
        return coord;

       /* return [
            {location: point, weight: 30},

            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),
            new google.maps.LatLng(34, -122),

            new google.maps.LatLng(37.782551, -122.445368),
            new google.maps.LatLng(37.782745, -122.444586),
            new google.maps.LatLng(37.782842, -122.443688),
            new google.maps.LatLng(37.782919, -122.442815),
            new google.maps.LatLng(37.782992, -122.442112),
            new google.maps.LatLng(37.783100, -122.441461),
            new google.maps.LatLng(37.783206, -122.440829),
            new google.maps.LatLng(37.783273, -122.440324),
            new google.maps.LatLng(37.783316, -122.440023),
            new google.maps.LatLng(47.783316, -112.440023),
            new google.maps.LatLng(47.783316, -112.440023),
            new google.maps.LatLng(47.783316, -112.440023),

        ];*/
    }
</script>
<script async defer
        src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBT4GdeltgR4mRXZ-H_U_k5g-ODgXG8INA&libraries=visualization&callback=initMap">
</script>
<script>
    function refresh() {
        window.location.reload();
    }
    setTimeout('refresh()', 10000);
</script>
</body>
</html>

