
// Misc. Utilities

const onlyOne = (iterable) => {
    console.assert(iterable.length == 1, `Unexpected number of elements in ${iterable}.`);
    return iterable[0];
};

// Initializations

const initializeQuestionToggleEffect = () => {
    const qaBlockDivs = document.getElementsByClassName('qa-block');
    Array.prototype.forEach.call(qaBlockDivs, (qaBlockDiv) => {
        const qaQuestionDiv = onlyOne(qaBlockDiv.getElementsByClassName('qa-question'));
        const qaQuestionAnchor = onlyOne(qaQuestionDiv.getElementsByTagName('a'));
        const qaAnswerDiv = onlyOne(qaBlockDiv.getElementsByClassName('qa-answer'));
        qaQuestionAnchor.addEventListener('click', function() {
            if (qaAnswerDiv.style.display === 'block') {
                qaAnswerDiv.style.display = 'none';
            } else {
                qaAnswerDiv.style.display = 'block';
            }
        });
    });
};

initializeQuestionToggleEffect(); 
