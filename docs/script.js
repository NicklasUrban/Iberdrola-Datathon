document.addEventListener('DOMContentLoaded', () => {
    const tabButtons = document.querySelectorAll('.tab-button');
    const mapFrame = document.getElementById('map-frame');

    const maps = {
        'map1': 'maps/map_1_current_stations.html',
        'map2': 'maps/map_2_demand_before.html',
        'map3': 'maps/map_3_proposed_stations.html',
        'map4': 'maps/map_4_demand_after.html',
        'map5': 'table.html'
    };

    tabButtons.forEach(button => {
        button.addEventListener('click', () => {
            // Remove active class from all buttons
            tabButtons.forEach(btn => btn.classList.remove('active'));
            
            // Add active class to clicked button
            button.classList.add('active');
            
            // Update iframe source
            const target = button.getAttribute('data-target');
            if (maps[target]) {
                // Add a small delay to allow CSS fade animation to trigger
                mapFrame.style.opacity = '0';
                setTimeout(() => {
                    mapFrame.src = maps[target];
                    mapFrame.style.opacity = '1';
                }, 150);
            }
        });
    });
});
