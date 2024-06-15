// Mock implementations of the random data generating functions
function generateRandomdata1() {
    return Math.random();
}

function generateRandomdata2() {
    return Math.random();
}

function generateRandomdata3() {
    return Math.random();
}

// Array to store the data
let allData = [];

// Function to continuously generate and store data
function collectData() {
    // Generate data from each function
    let data1 = generateRandomdata1();
    let data2 = generateRandomdata2();
    let data3 = generateRandomdata3();

    // Push the generated data as an object into the allData array
    allData.push({ data1, data2, data3 });

    // Log the collected data to see the progress
    console.log('All Data:', allData);

    // Call the function again after a short delay
    setTimeout(collectData, 1000); // Adjust the delay as needed
}

// Start collecting data
collectData();

