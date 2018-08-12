<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="initial-scale=1.0" />
    <style type="text/css">
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
    </style>
</head>
<body id="map-container">
<div id="map"></div>
<script>
    var map;

    function initMap() {
        map = new google.maps.Map(document.getElementById('map'), {
            zoom: 4,
            center: {lat: 41.22, lng: -100.68},
            styles: mapStyle
        });

        map.data.setStyle(styleFeature);

        // Get the earthquake data (JSONP format)
        // This feed is a copy from the USGS feed, you can find the originals here:
        //   http://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php

        var script = document.createElement('script');
        script.setAttribute(
            'src',
            './get_data.js');
        document.getElementsByTagName('head')[0].appendChild(script);
    }

    // Defines the callback function referenced in the jsonp file.
    function add_data() {
        var points = ${data_points};

        var data = {"type":"FeatureCollection"};
        /*
                {"type":"Feature","properties":{"mag":3.3}, "geometry":{"type":"Point","coordinates":[-148.1671,62.3922,38.2]}},
                {"type":"Feature","properties":{"mag":2.4}},
                {"type":"Feature","properties":{"mag":1.9480}},
                {"type":"Feature","properties":{"mag":1.5}},
                {"type":"Feature","properties":{"mag":1.9}},
                {"type":"Feature","properties":{"mag":3}},
                {"type":"Feature","properties":{"mag":1.2}},
                {"type":"Feature","properties":{"mag":1.6}},
                {"type":"Feature","properties":{"mag":1.5}},
                {"type":"Feature","properties":{"mag":2.2}},
                {"type":"Feature","properties":{"mag":1.2}},
                {"type":"Feature","properties":{"mag":2.3}}],
        };*/

        var feature = new Array(points.length);

        for (i=0; i<points.length; i++) {
            lat = points[i]["lat"];
            lng = points[i]["lng"];
            c = points[i]["count"];
            s = points[i]["sent"];

            feature[i] = {"type":"Feature",
                "properties":{"count":c, "sent": s},
                "geometry":{"type":"Point","coordinates":[lng, lat]}
            };
        }

        data["features"] = feature;

        console.log(data);
        map.data.addGeoJson(data);
    }

    function styleFeature(feature) {
        var low = [151, 83, 34];   // color of mag 1.0
        var high = [5, 69, 54];  // color of mag 6.0 and above
        var minMag = 0.0;
        var maxMag = 1.0;

        // fraction represents where the value sits between the min and max
        var fraction = (Math.min(feature.getProperty('sent'), maxMag) - minMag) /
            (maxMag - minMag);

        var color = interpolateHsl(low, high, fraction);

        return {
            icon: {
                path: google.maps.SymbolPath.CIRCLE,
                strokeWeight: 0.5,
                strokeColor: '#fff',
                fillColor: color,
                fillOpacity: 0.35,
                // while an exponent would technically be correct, quadratic looks nicer
                scale: feature.getProperty('count') * 10
            },
            zIndex: Math.floor(feature.getProperty('sent'))
        };
    }

    function interpolateHsl(lowHsl, highHsl, fraction) {
        var color = [];
        for (var i = 0; i < 3; i++) {
            // Calculate color based on the fraction.
            color[i] = (highHsl[i] - lowHsl[i]) * fraction + lowHsl[i];
        }

        return 'hsl(' + color[0] + ',' + color[1] + '%,' + color[2] + '%)';
    }

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
</script>
<script async defer
        src="https://maps.googleapis.com/maps/api/js?key=AIzaSyBT4GdeltgR4mRXZ-H_U_k5g-ODgXG8INA&callback=initMap">
</script>
</body>
</html>