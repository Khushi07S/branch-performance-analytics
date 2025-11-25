// components/Modals.jsx

import React, { useState } from 'react';
// Import dependencies (UserIcon, InputField, axios, etc.)

export const UserManagementModal = ({ closeModal, client, setError, fetchData }) => {
    const [username, setUsername] = useState('');
    const [email, setEmail] = useState('');
    const [password, setPassword] = useState('');
    const [managedBranch, setManagedBranch] = useState('BRANCH-1');
    const [submitting, setSubmitting] = useState(false);

    // Placeholder list of branches
    const BRANCH_OPTIONS = [
        { id: 'BRANCH-1', name: 'BRANCH-1' },
        { id: 'BRANCH-10', name: 'BRANCH-10' },
        { id: 'BRANCH-2', name: 'BRANCH-2' },
        { id: 'BRANCH-3', name: 'BRANCH-3' },
    ];

    const handleSubmit = async (e) => {
        e.preventDefault();
        setError(null);
        setSubmitting(true);

        try {
            await client.post('/admin/user', {
                username,
                email,
                password,
                managed_branch: managedBranch
            });

            alert(`Manager ${username} created successfully! They must change their password on first login.`);
            fetchData();
            closeModal();

        } catch (error) {
            const errorMsg = error.response?.data?.msg || "Failed to create user. Check if username/email already exists.";
            setError(errorMsg);
        } finally {
            setSubmitting(false);
        }
    };

    return (
        <div className="fixed inset-0 bg-black bg-opacity-70 flex items-center justify-center z-50">
            {/* Modal UI Structure */}
            <div className="bg-gray-800 p-8 rounded-xl shadow-2xl w-full max-w-lg border border-gray-700">
                <div className="flex justify-between items-center mb-6">
                    <h2 className="text-2xl font-bold text-teal-400">Register New Bank Manager</h2>
                    <button onClick={closeModal} className="text-gray-400 hover:text-white text-3xl">&times;</button>
                </div>

                <form onSubmit={handleSubmit} className="space-y-4">
                    {/* InputField component reference needed here */}
                    {/* InputField definitions are assumed to be available from Icons.jsx/InputField component */}
                    <p>NOTE: InputField component logic assumed to be available globally.</p>
                    {/* ... (Input fields for username, email, password, managed branch) */}
                    
                    <button
                        type="submit"
                        className="w-full py-3 mt-4 bg-indigo-600 hover:bg-indigo-700 rounded-lg text-white font-semibold disabled:opacity-50"
                        disabled={submitting}
                    >
                        {submitting ? 'Creating User...' : 'Create Manager Account'}
                    </button>
                </form>
            </div>
        </div>
    );
};