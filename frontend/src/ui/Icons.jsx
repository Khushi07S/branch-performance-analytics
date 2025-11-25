// components/Icons.jsx

import React from 'react';

export const BankIcon = ({ className }) => (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-2m-2 0h-2m2 0H5m5 0v-8m0 0a4 4 0 014-4h2m-2 4h-2m-2 0a4 4 0 01-4-4v-4m4 8h-2" />
    </svg>
);

export const UserIcon = ({ className }) => (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
    </svg>
);
export const EyeIcon = ({ className }) => (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12.324a.71.71 0 010-.648C3.89 8.243 7.85 5 12 5s8.11 3.243 9.542 6.676a.71.71 0 010 .648C20.11 15.757 16.15 19 12 19s-8.11-3.243-9.542-6.676z" />
    </svg>
);

export const EyeOffIcon = ({ className }) => (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13.875 18.828c-1.353 0-2.673-.324-3.875-.972-1.745-1.047-3.08-2.73-3.875-4.672M10 12a2 2 0 11-4 0 2 2 0 014 0zm6 0a2 2 0 11-4 0 2 2 0 014 0z" />
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M2.458 12.324a.71.71 0 010-.648C3.89 8.243 7.85 5 12 5s8.11 3.243 9.542 6.676a.71.71 0 010 .648C20.11 15.757 16.15 19 12 19s-8.11-3.243-9.542-6.676z" />
    </svg>
);
// Helper component for input fields (used across Auth/Modal screens)
export const InputField = ({ label, type, value, onChange, required }) => {
    const isPassword = type === 'password';
    const [isVisible, setIsVisible] = useState(false);

    const inputType = isPassword ? (isVisible ? 'text' : 'password') : type;

    return (
        // *** CRITICAL FIX: Make the parent div RELATIVE ***
        <div className="relative"> 
            <label className="block text-sm font-medium text-gray-300 mb-1">{label}</label>
            <input
                type={inputType}
                value={value}
                onChange={onChange}
                // Add padding-right to make space for the icon
                className="w-full px-4 py-2 pr-10 bg-gray-700 border border-gray-600 rounded-lg text-white"
                required={required}
            />
            {isPassword && (
                <button
                    type="button"
                    onClick={() => setIsVisible(!isVisible)}
                    // Positioning the button ABSOLUTELY inside the relative parent
                    // We use top-1/2 and -translate-y-1/2 to perfectly center the icon vertically
                    className="absolute right-0 top-1/2 transform -translate-y-1/2 flex items-center px-3 text-gray-400 hover:text-teal-400 focus:outline-none"
                    // Override default button styles which often have unwanted margins
                    style={{ background: 'none', border: 'none' }} 
                >
                    {isVisible ? (
                        <EyeOffIcon className="w-5 h-5" />
                    ) : (
                        <EyeIcon className="w-5 h-5" />
                    )}
                </button>
            )}
        </div>
    );
};
