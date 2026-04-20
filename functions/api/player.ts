async function someFunction() {
    let errorResponse;
    try {
        // Your logic here
    } catch (error) {
        errorResponse = { message: 'An error occurred', error: error.message };
        return errorResponse;
    }
}