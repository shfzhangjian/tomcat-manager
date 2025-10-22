/**
 * Gets a DOM element by its ID.
 * @param {string} id The ID of the element.
 * @returns {HTMLElement}
 */
export function D(id) {
    return document.getElementById(id);
}

/**
 * Debounce function to limit the rate at which a function gets called.
 * This is useful for event listeners that fire frequently, like 'input' or 'resize'.
 * @param {Function} func The function to debounce.
 * @param {number} delay The delay in milliseconds.
 * @returns {Function} A new debounced function.
 */
export function debounce(func, delay) {
    let timeout;
    return function(...args) {
        const context = this;
        clearTimeout(timeout);
        timeout = setTimeout(() => func.apply(context, args), delay);
    };
}

