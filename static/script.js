// -- API functions --
// Pass the group of data requested and returns it
async function fetchDataGroup(group) {
    try {
        const response = await fetch(`/api/data?group=${group}`);
        const data = await response.json();
        return (data.data);
    } catch (error) {
        console.log(`Error fetching ${group} data:`, error);
    }
}

// Pass the group of data requested and returns the latest 100 rows from db
async function fetchDataGroupLimited(group, limit) {
    try {
        const response = await fetch(`/api/data?group=${group}&limit=${limit}`);
        const data = await response.json();
        return (data.data);
    } catch (error) {
        console.log(`Error fetching ${group} data with limit ${limit}:`, error);
    }
}

// Pass a substring and returns the last row from the db with the substring
async function fetchDataSingular(substr) {
    try {
        const response = await fetch(`/api/data/live?id=${substr}`);
        const data = await response.json();
        return (data.data);
    } catch (error) {
        console.log(`Error fetching ${substr} data:`, error);
    }
}

async function fetchDataTable(nodeId)   {
    try {
        const response = await fetch(`/api/data/table?nodeId=${nodeId}`);
        const data = await response.json();
        return (data.data);
    } catch (error) {
        console.log(`Error fetching ${nodeId} data:`, error);
    }
}

function createLabels(data, motor) {
    const motorData = data.filter(row => {
        const isMotor = isMotorNodeId(row.NodeId, motor);
        //console.log(`Checking NodeId ${row.NodeId} for Motor ${motor}: ${isMotor}`);
        return isMotor;
    });
    const labels = Array.from(motorData, (row) => row.ServerTimeStamp);
    return labels;
}

// -- Chart functions --
// C: Creates ??datasets??
function createDataset(data, motors) {
    const datasets = [];
    const colors = {
        X: '#FFC3A0', // Soft green-blue
        Y: '#A0FFC3', // Soft orange
        Z: '#A0C3FF', // Soft lavender
        Spindle: '#e78ac3', // Soft pink
        'Spindle 1': '#e78ac3',
        'Spindle 2': '#c78ae3',
        'Spindle 3': '#a78ae3'
    };

    // Splits motor data 
    motors.forEach((motor) => {
        // Filter data for the current motor
        const motorData = data.filter(row => {
            const isMotor = isMotorNodeId(row.NodeId, motor);
            //console.log(`Checking NodeId ${row.NodeId} for Motor ${motor}: ${isMotor}`);
            return isMotor;
        });
        //console.log(`Filtered data for ${motor}:`, motorData);

        if (motorData.length > 0) {
            datasets.push({
                label: `Motor ${motor}`,
                borderColor: colors[motor],
                data: motorData.map(row => row.Value)
            });
        } else {
            console.log(`No data found for ${motor}.`);
        }
    });
    //console.log('Final datasets:', datasets);
    return datasets;
}

// Function to update the line chart with data
function createLineChart(canvasId, labels, datasets, chartTitle) {
    const canvas = document.getElementById(canvasId);
    const ctx = canvas.getContext('2d');

    // Find the existing instance of the chart with the same canvas ID
    const existingChart = Chart.getChart(ctx);

    // Destroy the existing chart with the same canvas ID
    if (existingChart) {
        existingChart.destroy();
    }

    // Create line chart with datasets and animation
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: datasets.filter(dataset => dataset !== null), // Filter out null datasets
        },
        options: {
            scales: {
                x: {
                    type: 'timeseries',
                    title: { 
                        display: 'true',
                        align: 'center',
                        text: 'Time (mm:ss)',
                        color: 'white',
                        padding: 8,
                        font:   {
                            size: 13
                        }
                    },
                    position: 'bottom',
                    ticks: {
                        maxTicksLimit: 11,
                        source: 'auto',
                        color: 'white'
                    },
                    time:   {
                        unit: 'second',
                        displayFormats: {
                            second: 'mm:ss'
                        }
                    }
                },
                y: {
                    type: 'linear',
                    position: 'left',
                    ticks: {
                        color: 'white', // Set y-axis label color
                    }
                }
            },
            animation: {
                duration: 200, // Set the duration of the animation in milliseconds
                easing: 'linear', // Set the easing function for the animation
            },
            plugins: {
                title: {
                    display: true,
                    text: chartTitle, // Update title here
                    color: '#fff', // Set title text color
                    font: {
                        size: 22, // Set the font size for the title
                    }
                },
                legend: {
                    labels: {
                        color: '#fff', // Set legend text color
                    },
                    onClick: (e) => e.stopPropagation()
                }
            }
        }
    });
}

function shiftLineChartData(canvasId, label, newData) {
    const canvas = document.getElementById(canvasId);
    const ctx = canvas.getContext('2d');
    // Find the existing instance of the chart with the same canvas ID
    const chart = Chart.getChart(ctx);
    // Iterate through the labels of datasets,
    // if match occurs with 'label, push data into dataset
    for(var i = 0; i < chart.data.datasets.length; i++) {
        if(chart.data.datasets[i].label.includes(label))    {
            chart.data.datasets[i].data.shift();
            chart.data.datasets[i].data.push(newData[0].Value);
            break;
        }
    }
    chart.update();
}

function datasetFilter(canvasId, button)    {
    const canvas = document.getElementById(canvasId);
    const ctx = canvas.getContext('2d');
    // Find the existing instance of the chart with the same canvas ID
    const chart = Chart.getChart(ctx);
    const chartDatasets = [];
    // Create array to store types of datasets in chart using label
    for(var i = 0; i < chart.data.datasets.length; i++)   {
        chartDatasets.push(chart.data.datasets[i].label);
    }
    // Check if dataset specified by button, as a substring, exists in chart
    // If doesn't exist, return
    const datasetExists = chartDatasets.some(setType => setType.includes(button));
    if(!datasetExists)
        return
    // Find index of dataset, then toggle visibility
    const datasetIndex = chartDatasets.findIndex(setType => setType.includes(button));
    if(chart.isDatasetVisible(datasetIndex))
        chart.setDatasetVisibility(datasetIndex, false);
    else
        chart.setDatasetVisibility(datasetIndex, true);
    chart.update();
}

// -- Filtering Functions --
function isMotorNodeId(nodeId, motor) {
    const motorMap = {
        X: ['Current1', 'CYCCNT1', 'MAXCUR21', 'MACPOS1', 'MATPOS1', 'SCAPOS1', 'RemainCommand1', 'CurrentPosition1', 'ServoMonitor_Gain1', 'ServoMonitor_Droop1', 'Speed1'],
        Y: ['Current2', 'CYCCNT2', 'MAXCUR22', 'MACPOS2', 'MATPOS2', 'SCAPOS2', 'RemainCommand2', 'CurrentPosition2', 'Gain2', 'Droop2', 'Speed1'],
        Z: ['Current3', 'CYCCNT3', 'MAXCUR23', 'MACPOS3', 'MATPOS3', 'SCAPOS3', 'RemainCommand3', 'CurrentPosition3', 'Gain3', 'Droop3', 'Speed1'],
        Spindle: ['SpindleMonitor_Gain1', 'SpindleMonitor_Droop1', 'LEDDisplay1', 'ControlInput11', 'CycleCount1', 'Load1'], // Include the node key for the 'Spindle' motor
        'Spindle 1': ['ControlOutput11'],
        'Spindle 2': ['ControlOutput21'],
        'Spindle 3': ['ControlOutput41'] 
    };

    // Check if motor is defined before accessing its properties
    if (motor in motorMap) {
        // If the nodeId is included in the associated motor list, return true. Otherwise, false.
        return motorMap[motor].some(v => nodeId.includes(v));
    }

    return false;
}

// -- For updating latest values on each page --
function updateCurrentValues(data) {
    const X = findLatestValue(data.filter(row => row.NodeId.includes('Current1')));
    const Y = findLatestValue(data.filter(row => row.NodeId.includes('Current2')));
    const Z = findLatestValue(data.filter(row => row.NodeId.includes('Current3')));

    document.getElementById('latest-X').innerText = X;
    document.getElementById('latest-Y').innerText = Y;
    document.getElementById('latest-Z').innerText = Z;
}

function updateCycleCountValues(data) {
    const X = findLatestValue(data.filter(row => row.NodeId.includes('CYCCNT1')));
    const Y = findLatestValue(data.filter(row => row.NodeId.includes('CYCCNT2')));
    const Z = findLatestValue(data.filter(row => row.NodeId.includes('CYCCNT3')));

    document.getElementById('latest-X').innerText = X;
    document.getElementById('latest-Y').innerText = Y;
    document.getElementById('latest-Z').innerText = Z;
    //document.getElementById('latest-Spindle').innerText = findLatestValue(data.filter(row => row.NodeId == `Current1`));
}

function findLatestValue(filteredData) {
    let latestValue = 0;
    for (let time_t0 = 0, i = 0; i < filteredData.length; i++) {
        let time_t1 = filteredData[i].ServerTimeStamp;
        if (time_t1 > time_t0)
            latestValue = filteredData[i].Value;
        time_t0 = time_t1;
    }
    return latestValue;
}

// This function will remain unused
function updateLatestValues(data, servoKey, spindleKey) {
    const XNewVal = findLatestValue(data.filter(row => row.NodeId == `${servoKey}1`));
    const YNewVal = findLatestValue(data.filter(row => row.NodeId == `${servoKey}2`));
    const ZNewVal = findLatestValue(data.filter(row => row.NodeId == `${servoKey}3`));
    const spindleNewVal = findLatestValue(data.filter(row => row.NodeId == `${spindleKey}1`));

    document.getElementById('latest-Spindle').innerText = spindleNewVal;
    document.getElementById('latest-X').innerText = XNewVal;
    document.getElementById('latest-Y').innerText = YNewVal;
    document.getElementById('latest-Z').innerText = ZNewVal;
}

// -- CSV functions --
// Function to convert data to CSV format
function convertToCSV(data) {
    const csv = [];
    const header = Object.keys(data[0]);
    csv.push(header.join(','));

    data.forEach(row => {
        const values = header.map(key => row[key]);
        csv.push(values.join(','));
    });

    return csv.join('\n');
}

// Function to trigger CSV download
function downloadCSV() {
    // Get the table data
    const table = document.getElementById('data-table');
    const rows = table.getElementsByTagName('tr');
    const data = [];

    // Iterate through rows and cells to collect data
    for (let i = 1; i < rows.length; i++) {
        const row = rows[i];
        const cells = row.getElementsByTagName('td');
        const rowData = {};

        for (let j = 0; j < cells.length; j++) {
            const cell = cells[j];
            const header = table.rows[0].cells[j].innerText;
            rowData[header] = cell.innerText;
        }

        data.push(rowData);
    }

    // Convert data to CSV format
    const csvContent = 'data:text/csv;charset=utf-8,' + encodeURIComponent(convertToCSV(data));

    // Create a hidden link element and trigger the download
    const link = document.createElement('a');
    link.href = csvContent;
    link.download = 'table_data.csv';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}