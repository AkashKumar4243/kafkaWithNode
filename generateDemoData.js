function generateRandomData() {
    const objects = [];
    for (let i = 0; i < 100; i++) {
        const nameLength = Math.floor(Math.random() * 6) + 5; // Random length between 5 and 10
        const chargerNameLength = Math.floor(Math.random() * 6) + 5; // Random length between 5 and 10
        
        const name = Array.from({ length: nameLength }, () => 
            String.fromCharCode(Math.floor(Math.random() * 26) + 97) // Random lowercase letter
        ).join('');
        
        const chargerName = Array.from({ length: chargerNameLength }, () => 
            String.fromCharCode(Math.floor(Math.random() * 26) + 97) // Random lowercase letter
        ).join('');

        const price = (Math.random() * (10.0 - 10.0) + 10.0).toFixed(2); // Random price between 10.0 and 1000.0
        
        objects.push({
            name: name,
            chargerName: chargerName,
            price: parseFloat(price)
        });
    }
    return objects;
}

export default generateRandomData;

// Example usage
// setInterval(() =>{
//     const randomObjects = generateRandomData();
//     console.log(randomObjects.length);
// }, 2000);
